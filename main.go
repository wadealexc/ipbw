package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-datastore"
	badger "github.com/ipfs/go-ds-badger"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/libp2p/go-libp2p-core/test"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	ma "github.com/multiformats/go-multiaddr"
)

// Workers are distinct goroutines used for querying the DHT
const Workers = 8

// Server is the location to which the crawler reports runs to
const Server = "http://127.0.0.1:8000/crawl"

var (
	duration = flag.Uint("d", 0, "(Optional) Number of minutes to crawl. No limit by default.")
)

// Crawler holds the information about an active crawl
type Crawler struct {
	// IPFS / libp2p fields
	host host.Host
	dht  *dht.IpfsDHT
	ds   datastore.Batching
	id   *identify.IDService

	// We receive QueryEvents from the DHT on this channel
	events <-chan *routing.QueryEvent
	// Result aggregator
	report *Report

	// Context and cancel function passed to DHT queries
	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup
}

// Report aggregates results from QueryEvents
type Report struct {
	// IDs maps a reporter peer to the peer IDs they told us about
	IDs map[peer.ID][]peer.ID
	// IPs maps a peer's ID to known IP addresses at which they can be found
	IPs map[peer.ID][]string
}

// ReportJSON is a json-marshallable form of Report
type ReportJSON struct {
	IDs map[string][]string
	IPs map[string][]string
}

// NewCrawler creates an IPFS crawler and starts our libp2p node.
func NewCrawler() *Crawler {
	fmt.Println("IPBW: Starting crawler")

	// Create a context that, when passed to DHT queries, emits QueryEvents to this channel
	ctx, cancel := context.WithCancel(context.Background())
	ctx, events := routing.RegisterForQueryEvents(ctx)

	c := &Crawler{
		ctx:    ctx,
		cancel: cancel,
		events: events,
		report: &Report{
			IDs: make(map[peer.ID][]peer.ID),
			IPs: make(map[peer.ID][]string),
		},
	}

	c.initHost()
	return c
}

// Start our libp2p node
func (c *Crawler) initHost() {
	var err error
	c.host, err = libp2p.New(context.Background())
	if err != nil {
		panic(err)
	}

	// Create a DB for our DHT client
	c.ds, err = badger.NewDatastore("dht.db", nil)
	if err != nil {
		panic(err)
	}

	// Create DHT client
	c.dht = dht.NewDHTClient(c.ctx, c.host, c.ds)

	// Bootstrap from bootstrap peers
	for _, a := range dht.DefaultBootstrapPeers {
		pi, err := peer.AddrInfoFromP2pAddr(a)
		if err != nil {
			panic(err)
		}

		// Connect to each bootstrap peer w/ 5 second timeout
		ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
		if err = c.host.Connect(ctx, *pi); err != nil {
			fmt.Printf("skipping bootstrap peer: %s\n", pi.ID.Pretty())
		}

		cancel()
	}

	c.id = identify.NewIDService(c.host)
}

// Spawn workers to begin querying DHT
// Spawn an aggregator to collect results from c.events
// Spawn a reporter to print incremental status updates
func (c *Crawler) start(numWorkers int) {
	fmt.Printf("Spawning %d workers\n", numWorkers)

	for i := 0; i < numWorkers; i++ {
		c.wg.Add(1)
		go c.worker()
	}

	go c.aggregator()
	go c.reporter()
}

// Called when we finish a crawl. Kills our goroutines and posts results to our server
func (c *Crawler) close() {
	c.cancel()
	c.wg.Done()

	c.finalizeReport()
	c.publishResults()

	if err := c.ds.Close(); err != nil {
		fmt.Printf("error while shutting down: %v\n", err)
	}
}

// Each worker generates a random peer ID, then queries the DHT
// Workers time out after 10 seconds and generate a new ID to query
// Results are received via the c.events channel
func (c *Crawler) worker() {
Work:
	// Get a random peer ID
	id, err := test.RandPeerID()
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	_, _ = c.dht.FindPeer(ctx, id)
	cancel()
	goto Work
}

// Incrementally prints status updates
func (c *Crawler) reporter() {
	var elapsed int = 0
	for {
		select {
		case <-time.After(10 * time.Second):
			elapsed += 10
			fmt.Printf("--- %d unique peers", len(c.report.IDs))
			if elapsed == 60 {
				// This is very important
				fmt.Printf("; time elapsed: %d minute\n", elapsed/60)
			} else if elapsed%60 == 0 {
				fmt.Printf("; time elapsed: %d minutes\n", elapsed/60)
			} else {
				fmt.Printf("\n")
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// Receives and filters QueryEvents from the DHT
func (c *Crawler) aggregator() {
	// Get our PeerID, so we can filter responses that come from our own node
	self := c.dht.PeerID()

	for report := range c.events {
		// We only care about PeerResponse events, as these
		// are peers informing us of peers they know.
		if report.Type != routing.PeerResponse {
			continue
		}

		// Skip responses from our node
		if self.String() == report.ID.String() {
			continue
		}

		// Filter out responses - we don't want our own node
		queryResponses := filterSelf(self, report.Responses)
		// No remaining responses? Skip.
		if len(queryResponses) == 0 {
			continue
		}

		seenID := map[peer.ID]struct{}{}
		var reportedIDs []peer.ID
		// Iterate over existing results and track which IDs we've seen:
		for _, id := range c.report.IDs[report.ID] {
			// Filter dupes
			if _, exists := seenID[id]; !exists {
				seenID[id] = struct{}{}
				reportedIDs = append(reportedIDs, id)
			}
		}

		// Response: AddrInfo { ID peer.ID; Addrs []MultiAddr }
		for _, r := range queryResponses {
			// Filter dupes
			if _, exists := seenID[r.ID]; !exists {
				seenID[r.ID] = struct{}{}
				reportedIDs = append(reportedIDs, r.ID)
			}

			// Filter response multiaddrs for IP+port combinations
			responseIPs := filterIPs(r.Addrs)
			if len(responseIPs) == 0 {
				continue // found nothing; skip
			}

			seenIP := map[string]struct{}{}
			var reportedIPs []string
			// Iterate over existing IPs and track which ones we've seen:
			for _, ip := range c.report.IPs[r.ID] {
				// Filter dupes
				if _, exists := seenIP[ip]; !exists {
					seenIP[ip] = struct{}{}
					reportedIPs = append(reportedIPs, ip)
				}
			}

			// Now, add newly-reported IPs to our report:
			for _, ip := range responseIPs {
				if _, exists := seenIP[ip]; !exists {
					seenIP[ip] = struct{}{}
					reportedIPs = append(reportedIPs, ip)
				}
			}

			if len(reportedIPs) != 0 {
				c.report.IPs[r.ID] = reportedIPs
			}
		}

		if len(reportedIDs) != 0 {
			c.report.IDs[report.ID] = reportedIDs
		}
	}
}

// Do a final pass over our reporter IDs and check the DHT PeerStore to see
// if we know of any additional IPs for them
func (c *Crawler) finalizeReport() {
	fmt.Println("Finalizing report...")
	pStore := c.host.Peerstore()

	for id := range c.report.IDs {
		// Try and find addresses for this peer
		addrs := pStore.Addrs(id)

		// Filter for ip+port combinations
		reporterIPs := filterIPs(addrs)
		if len(reporterIPs) == 0 {
			continue // found nothing; skip :(
		}

		seenIP := map[string]struct{}{}
		var resultIPs []string
		// Iterate over existing IPs and track what we've seen:
		for _, ip := range c.report.IPs[id] {
			// Filter dupes
			if _, exists := seenIP[ip]; !exists {
				seenIP[ip] = struct{}{}
				resultIPs = append(resultIPs, ip)
			}
		}

		// Now, add newly-filtered IPs to our report:
		for _, ip := range reporterIPs {
			// Filter dupes
			if _, exists := seenIP[ip]; !exists {
				seenIP[ip] = struct{}{}
				resultIPs = append(resultIPs, ip)
			}
		}

		if len(resultIPs) != 0 {
			c.report.IPs[id] = resultIPs
		}
	}
}

// Send c.report to server
func (c *Crawler) publishResults() {
	fmt.Printf("Sending results to server at %s\n", Server)

	results := &ReportJSON{
		IDs: make(map[string][]string),
		IPs: make(map[string][]string),
	}

	// Prettyify reported IDs and add to results
	for source, targets := range c.report.IDs {
		targetsPretty := make([]string, 0)
		for _, target := range targets {
			targetsPretty = append(targetsPretty, target.Pretty())
		}

		results.IDs[source.Pretty()] = targetsPretty
	}

	// Prettyify reported target IDs and add to results
	for peer, addrs := range c.report.IPs {
		results.IPs[peer.Pretty()] = addrs
	}

	reportBody, err := json.Marshal(results)
	if err != nil {
		panic(err) // not even gonna try and recover lol
	}

	resp, err := http.Post(Server, "application/json", bytes.NewBuffer(reportBody))
	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(body))
}

// Iterates over a collection of multiaddrs and attempts to find ip+port combos
func filterIPs(addrs []ma.Multiaddr) []string {
	var results []string = make([]string, 0)
	for _, addr := range addrs {
		fields := strings.Split(addr.String(), "/")

		var (
			res       string
			portFound bool
			ipFound   bool
		)
		// Iterate over the split multiaddr. First, look for an IP (IPv4, IPv6, DNS4, DNS6)
		// Then, find a port (UDP, TCP)
		for i, field := range fields {
			if field == "ip4" || field == "ip6" || field == "dns4" || field == "dns6" || field == "dnsaddr" { // found ip
				if res != "" {
					fmt.Printf("Possible malformed multiaddr %s; skipping\n", addr.String())
					break
				} else if i+1 >= len(fields) {
					fmt.Printf("Unexpected EOField in %s; skipping\n", addr.String())
					break
				} else if fields[i+1] == "127.0.0.1" {
					// Skip localhost
					break
				}
				res = fields[i+1]
				ipFound = true
			} else if field == "udp" || field == "tcp" { // found port
				if res == "" {
					fmt.Printf("Possible malformed multiaddr %s; skipping\n", addr.String())
					break
				} else if i+1 >= len(fields) {
					fmt.Printf("Unexpected EOField in %s; skipping\n", addr.String())
					break
				}
				res = res + ":" + fields[i+1]
				portFound = true
				break
			}
		}

		// Nice, we found a well-formed IP+Port! Add to results:
		if ipFound && portFound {
			results = append(results, res)
		}
	}
	return results
}

// Filter DHT query responses that contain our own peer ID
func filterSelf(self peer.ID, responsesRaw []*peer.AddrInfo) []*peer.AddrInfo {
	var res []*peer.AddrInfo = make([]*peer.AddrInfo, 0)
	for _, info := range responsesRaw {
		if info.ID.String() != self.String() {
			res = append(res, info)
		}
	}
	return res
}

func main() {
	// Silence that one annoying errorw from basichost.
	// I know, we have too many open files!
	lvl, err := logging.LevelFromString("DPANIC")
	if err != nil {
		panic(err)
	}
	logging.SetAllLoggers(lvl)

	// Parse CLI flags
	flag.Parse()

	crawler := NewCrawler()
	crawler.start(Workers)

	// Create a channel that will be notified of os.Interrupt or os.Kill signals
	// This way, if we stop the crawler (e.g. with ^C), we can exit gracefully.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)

	if *duration != 0 {
		// Duration specified - calculate total runtime
		totalDuration := time.Duration(*duration) * 60 * time.Second
		fmt.Printf("(Running %d minute crawl)\n", *duration)
		select {
		case <-time.After(totalDuration):
			crawler.close()
			return
		case <-ch:
			crawler.close()
			return
		case <-crawler.ctx.Done():
			return
		}
	} else {
		select {
		case <-ch:
			crawler.close()
			return
		case <-crawler.ctx.Done():
			return
		}
	}
}
