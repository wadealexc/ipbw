package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-datastore"
	badger "github.com/ipfs/go-ds-badger"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/libp2p/go-libp2p-core/test"
	host "github.com/libp2p/go-libp2p-host"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
)

// Workers are distinct goroutines used for querying the DHT
const Workers = 8

// PeerIDFile stores IDs of discovered peers, along with the ID
// of the peer who responded to our query. Records take the form:
// pOrigin,pResponse
// ... where each field is a peer ID
const PeerIDFile = "peerIDs.csv"

// IDToIPFile stores IPv4/IPv6/DNS4/DNS6 addresses of peers, along
// with the ID of the peer located at that address. Records take the form:
// pID,ip:port
const IDToIPFile = "peerIPs.csv"

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

	// Result aggregator and file/writer pairs for output
	results      map[peer.ID][]*peer.AddrInfo
	peerIDWriter *csv.Writer
	peerIDFile   *os.File
	IDToIPWriter *csv.Writer
	IDToIPFile   *os.File

	// Context and cancel function passed to DHT queries
	ctx    context.Context
	cancel context.CancelFunc
	// We receive QueryEvents from the DHT on this channel
	events <-chan *routing.QueryEvent

	wg sync.WaitGroup
}

// NewCrawler creates an IPFS crawler.
// It also opens CSV files for output and starts our libp2p node.
func NewCrawler() *Crawler {
	// Create a context that, when passed to DHT queries, emits QueryEvents to this channel
	ctx, cancel := context.WithCancel(context.Background())
	ctx, events := routing.RegisterForQueryEvents(ctx)

	c := &Crawler{
		results: make(map[peer.ID][]*peer.AddrInfo),
		ctx:     ctx,
		cancel:  cancel,
		events:  events,
	}

	c.initCSV()
	c.initHost()
	return c
}

// Create output files, as well as writers for those files
func (c *Crawler) initCSV() {
	file, err := os.Create(PeerIDFile)
	if err != nil {
		panic(err)
	}
	writer := csv.NewWriter(file)

	c.peerIDWriter = writer
	c.peerIDFile = file

	file, err = os.Create(IDToIPFile)
	if err != nil {
		panic(err)
	}
	writer = csv.NewWriter(file)

	c.IDToIPWriter = writer
	c.IDToIPFile = file
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
	fmt.Println("IPBW: Starting crawl")
	fmt.Printf("Spawning %d workers\n", numWorkers)

	for i := 0; i < numWorkers; i++ {
		c.wg.Add(1)
		go c.worker()
	}

	go c.aggregator()
	go c.reporter()

	// c.host.Network().Notify((*notifee)(c))
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
			fmt.Printf("--- %d unique peers", len(c.results))
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
	for e := range c.events {
		// We only care about PeerResponse events, as these
		// are peers informing us of peers they know.
		if e.Type == routing.PeerResponse {
			// Make sure we don't record responses from our own node!
			self := c.dht.PeerID()
			if self.String() == e.ID.String() {
				fmt.Printf("Self-report; skipping!\n")
				continue
			}
			// Filter out e.Responses - we don't want our own node
			queryResponses := filterSelf(self, e.Responses)
			// No remaining responses? Skip.
			if len(queryResponses) == 0 {
				continue
			}

			// If we haven't seen this peer yet, we don't need to think too hard
			// Just set the responses and continue!
			if _, exists := c.results[e.ID]; !exists {
				c.results[e.ID] = queryResponses
			} else {
				// Okay, we've seen this peer already!
				// Add new responses and filter out duplicates:
				seen := map[peer.ID]struct{}{}
				var responses []*peer.AddrInfo
				// Iterate over existing results and track which IDs we've seen:
				for _, r := range c.results[e.ID] {
					// filter dupes
					if _, exists := seen[r.ID]; exists {
						continue
					}

					seen[r.ID] = struct{}{}
					responses = append(responses, r)
				}

				// Iterate over incoming results and append IDs we haven't seen:
				for _, r := range queryResponses {
					// filter dupes
					if _, exists := seen[r.ID]; exists {
						continue
					}

					seen[r.ID] = struct{}{}
					responses = append(responses, r)
				}

				// Skip empty responses
				if len(responses) == 0 {
					continue
				}

				c.results[e.ID] = responses
			}
		}
	}
}

// Called when we finish a crawl. Kills our goroutines and writes output to a file.
func (c *Crawler) close() {
	c.cancel()
	c.wg.Done()

	c.writeToFile()

	if err := c.ds.Close(); err != nil {
		fmt.Printf("error while shutting down: %v\n", err)
	}
}

func (c *Crawler) writeToFile() {
	fmt.Println("Writing results to file...")

	// Make sure we close our files and flush our writers at the end of this method
	defer c.peerIDFile.Close()
	defer c.IDToIPFile.Close()

	defer c.peerIDWriter.Flush()
	defer c.IDToIPWriter.Flush()

	pStore := c.host.Peerstore()
	var numSkipped int = 0

	for pID, addrs := range c.results {
		// Try to get IPv4/IPv6 addresses for this peer
		pInfo := pStore.Addrs(pID)

		// Maybe we don't know ANY multiaddrs for this peer? Skip if so...
		if len(pInfo) == 0 {
			numSkipped++
			continue
		}

		var count int = 0
		// Iterate over []Multiaddr returned for our reporting peer (pID)
		// We want to find each IPv4 / IPv6 address and add it to ipFile
		for _, a := range pInfo {
			fields := strings.Split(a.String(), "/")
			var (
				res       string
				portFound bool
				ipFound   bool
			)
			for i, field := range fields {
				// Found IPv4/IPv6 or DNS; add to result
				if field == "ip4" || field == "ip6" || field == "dns4" || field == "dns6" {
					if res != "" {
						fmt.Printf("Possible malformed ip %s; skipping...\n", a.String())
						break
					} else if i+1 >= len(fields) {
						fmt.Printf("Unexpected EOField %s; skipping...\n", a.String())
						break
					} else if fields[i+1] == "127.0.0.1" {
						// Skip localhost
						break
					}
					res = fields[i+1]
					ipFound = true
				} else if field == "udp" || field == "tcp" { // Port found; add to result
					if res == "" {
						fmt.Printf("Possible malformed port %s; skipping...\n", a.String())
						break
					} else if i+1 >= len(fields) {
						fmt.Printf("Unexpected EOField %s; skipping...\n", a.String())
						break
					}
					res = res + ":" + fields[i+1]
					portFound = true
					break
				}
			}

			// Nice, we got an address! Write to file:
			if ipFound && portFound {
				count++
				result := make([]string, 2)
				result[0] = pID.Pretty()
				result[1] = res
				c.IDToIPWriter.Write(result)
			}
		}

		// So, we found multiaddrs for this peer, but didn't find IPv4 / IPv6? Weird... skip.
		if count == 0 {
			fmt.Printf("%s has %d maddrs, but no ip4/ip6/dns4/dns6!\nAddrs:%v\n", pID.Pretty(), len(pInfo), pInfo)
			continue
		}

		for _, addr := range addrs {
			result := make([]string, 2)
			result[0] = pID.Pretty()
			result[1] = addr.ID.Pretty()
			c.peerIDWriter.Write(result)
		}
	}

	fmt.Printf("Finishing write. Skipped %d peers with no known multiaddrs\n", numSkipped)
}

/**
 * Filter DHT query responses that contain our own peer ID
 */
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
