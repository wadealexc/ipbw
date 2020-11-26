package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
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
	duration       = flag.Uint("d", 5, "(Optional) Number of minutes to crawl. 5 minutes by default.")
	reportInterval = flag.Uint("i", 60, "(Optional) Number of seconds between updates sent to the server. 60s by default.")
)

// Crawler holds the information about an active crawl
type Crawler struct {
	// IPFS / libp2p fields
	host host.Host
	dht  *dht.IpfsDHT
	ds   datastore.Batching
	id   *identify.IDService

	// We receive QueryEvents from the DHT on this channel
	events         <-chan *routing.QueryEvent
	report         *Report // Result aggregator
	reportInterval uint    // How often a report is sent to the server

	// Context and cancel function passed to DHT queries
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Report aggregates results from QueryEvents
// IDs maps a reporter peer to the peer IDs they told us about
// IPs maps a peer's ID to known IP addresses at which they can be found
type Report struct {
	mu  sync.RWMutex
	IDs map[peer.ID][]peer.ID
	IPs map[peer.ID][]string
}

// ReportJSON is a json-marshallable form of Report
type ReportJSON struct {
	IDs map[string][]string
	IPs map[string][]string
}

// NewCrawler creates an IPFS crawler and starts our libp2p node.
// TODO: Ping server here to see if it's running
func NewCrawler(reportInterval uint) *Crawler {
	fmt.Println("IPBW: Starting crawler")

	// Create a context that, when passed to DHT queries, emits QueryEvents to this channel
	ctx, cancel := context.WithCancel(context.Background())
	ctx, events := routing.RegisterForQueryEvents(ctx)

	c := &Crawler{
		ctx:            ctx,
		cancel:         cancel,
		events:         events,
		reportInterval: reportInterval,
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

// Intermittently reports data to our server. Prints a log message for each report sent.
func (c *Crawler) reporter() {
	var elapsed uint = 0
	interval := time.Duration(c.reportInterval) * time.Second
	for {
		select {
		case <-time.After(interval):
			elapsed += c.reportInterval

			results, response := c.publishResults()

			if elapsed/60 == 1 {
				// This is very important
				fmt.Printf("--- Time elapsed: %d minute ---\n", elapsed/60)
			} else {
				fmt.Printf("--- Time elapsed: %d minutes ---\n", elapsed/60)
			}

			fmt.Printf("%d peers reporting:\n", results.reporters)
			fmt.Printf("- %d unique peer IDs -\n", results.uniquePeers)
			fmt.Printf("- %d peers with a valid IP+port -\n", results.peersWithIPs)
			fmt.Printf("- %d unique IP+port combinations -\n", results.uniqueTargets)
			fmt.Printf("Sent report to (%s) and got response: %s\n", Server, response)
			fmt.Printf("---\n")
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

		// Get reporting peer and the IDs they reported
		reporter := report.ID
		response := filterSelf(self, report)

		// No responses? Skip!
		if len(response) == 0 {
			continue
		}

		// Which IDs this reporter has reported
		var reportedIDs []peer.ID
		// Which peer+IPs this reporter has reported
		allReportedIPs := make(map[peer.ID][]string)

		// Iterate over previously-reported peers, and track what we've seen:
		seenID := map[peer.ID]struct{}{}
		c.report.mu.Lock()
		for _, peer := range c.report.IDs[reporter] {
			// Filter duplicates
			if _, seen := seenID[peer]; !seen {
				seenID[peer] = struct{}{}
				reportedIDs = append(reportedIDs, peer)
			}
		}
		c.report.mu.Unlock()

		// Iterate over the newly-reported peers+IPs:
		for _, peerInfo := range response {
			// Filter duplicates
			if _, seen := seenID[peerInfo.ID]; !seen {
				seenID[peerInfo.ID] = struct{}{}
				reportedIDs = append(reportedIDs, peerInfo.ID)
			}

			// Filter response multiaddrs for IP+port combinations
			responseIPs := filterIPs(peerInfo.Addrs)
			if len(responseIPs) == 0 {
				continue // found nothing; skip
			}

			// Which IPs we know for this peer
			var reportedIPs []string

			// Iterate over previously-reported IPs, and track what we've seen:
			seenIP := map[string]struct{}{}
			c.report.mu.Lock()
			for _, ip := range c.report.IPs[peerInfo.ID] {
				// Filter duplicates
				if _, seen := seenIP[ip]; !seen {
					seenIP[ip] = struct{}{}
					reportedIPs = append(reportedIPs, ip)
				}
			}
			c.report.mu.Unlock()

			// Iterate over newly-reported IPs:
			for _, ip := range responseIPs {
				// Filter duplicates
				if _, seen := seenIP[ip]; !seen {
					seenIP[ip] = struct{}{}
					reportedIPs = append(reportedIPs, ip)
				}
			}

			if len(reportedIPs) != 0 {
				allReportedIPs[peerInfo.ID] = reportedIPs
			}
		}

		// Do our writes, locking the map for reads
		c.report.mu.RLock()
		for peer, ips := range allReportedIPs {
			if len(ips) != 0 {
				c.report.IPs[peer] = ips
			}
		}

		if len(reportedIDs) != 0 {
			c.report.IDs[reporter] = reportedIDs
		}
		// Unlock map for reads
		c.report.mu.RUnlock()
	}
}

type Results struct {
	reporters     uint // Number of peers that responded to our queries
	uniquePeers   uint // Number of peers that we have an ID for
	peersWithIPs  uint // Number of peers we have an IP address for
	uniqueTargets uint // Number of unique IP+port combinations we have
}

// Send c.report to server
func (c *Crawler) publishResults() (Results, string) {
	reportJSON := &ReportJSON{
		IDs: make(map[string][]string),
		IPs: make(map[string][]string),
	}

	results := &Results{}

	seenID := map[peer.ID]struct{}{}
	seenIP := map[string]struct{}{}

	// Prettyify reported IDs and add to results
	c.report.mu.Lock()
	for source, targets := range c.report.IDs {
		// Tally number of reporters and unique peer IDs
		if _, seen := seenID[source]; !seen {
			seenID[source] = struct{}{}
			results.reporters++
			results.uniquePeers++
		}

		targetsPretty := make([]string, 0)
		for _, target := range targets {
			// Tally unique peer IDs
			if _, seen := seenID[target]; !seen {
				seenID[target] = struct{}{}
				results.uniquePeers++
			}

			targetsPretty = append(targetsPretty, target.Pretty())
		}

		reportJSON.IDs[source.Pretty()] = targetsPretty
	}

	// Prettyify reported target IDs and add to results
	for peer, ips := range c.report.IPs {
		results.peersWithIPs++

		// Tally unique IP+port combinations
		for _, ip := range ips {
			if _, seen := seenIP[ip]; !seen {
				seenIP[ip] = struct{}{}
				results.uniqueTargets++
			}
		}

		reportJSON.IPs[peer.Pretty()] = ips
	}
	c.report.mu.Unlock()

	reportBody, err := json.Marshal(reportJSON)
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

	return *results, string(body)
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

				ipRaw := fields[i+1]

				// If we parsed a DNS address, we're done
				if field == "dns4" || field == "dns6" || field == "dnsaddr" {
					res = ipRaw
					ipFound = true
					break
				}

				// For IPv4 / IPv6 addresses, filter out private / local / loopback IPs
				ip := net.ParseIP(ipRaw)
				switch {
				case ip == nil:
					fmt.Printf("Invalid IP address in %s: %s; skipping\n", addr.String(), res)
					break
				case ip.IsUnspecified(), ip.IsLoopback(), ip.IsInterfaceLocalMulticast():
					break
				case ip.IsLinkLocalMulticast(), ip.IsLinkLocalUnicast():
					break
				}

				// Nice, we're done!
				res = ipRaw
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
func filterSelf(self peer.ID, report *routing.QueryEvent) []*peer.AddrInfo {
	var res []*peer.AddrInfo = make([]*peer.AddrInfo, 0)

	// We don't want reports from our own node
	if report.ID == self {
		return res
	}

	// We also don't want reports that contain our own node
	responsesRaw := report.Responses
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

	crawler := NewCrawler(*reportInterval)
	crawler.start(Workers)

	// Create a channel that will be notified of os.Interrupt or os.Kill signals
	// This way, if we stop the crawler (e.g. with ^C), we can exit gracefully.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)

	// Calculate total runtime
	totalDuration := time.Duration(*duration) * 60 * time.Second
	fmt.Printf("Running %d minute crawl\n", *duration)
	fmt.Printf("Posting results every %d seconds\n", *reportInterval)
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
}
