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

// ServerPing is an endpoint we ping on crawler creation to make sure the server is running
const ServerPing = "http://127.0.0.1:8000/ping"

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
	numReporters   uint    // Number of unique peers reporting neighbors

	// Context and cancel function passed to DHT queries
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Peer contains all the info we know for a given peer
type Peer struct {
	ips       []string  // All known IPs/ports for this peer
	neighbors []peer.ID // All neighbors this peer reported to us
	timestamp string    // The UTC timestamp when we discovered the peer
}

// Report aggregates results from QueryEvents
// IDs maps a reporter peer to the peer IDs they told us about
// IPs maps a peer's ID to known IP addresses at which they can be found
type Report struct {
	mu    sync.RWMutex
	peers map[peer.ID]*Peer
}

// IDs maps a reporter peer to the peer IDs they told us about
// IPs maps a peer's ID to known IP addresses at which they can be found
// type Report struct:
// 	mu  sync.RWMutex
// 	IDs map[peer.ID][]peer.ID
// 	IPs map[peer.ID][]string

// PeerJSON is a json-marshallable form of Peer (with an added pid field)
type PeerJSON struct {
	Pid       string   `json:"pid"`
	Ips       []string `json:"ips"`
	Neighbors []string `json:"neighbors"`
	Timestamp string   `json:"timestamp"`
}

// ReportJSON is a json-marshallable form of Report
type ReportJSON struct {
	Peers []PeerJSON `json:"peers"`
}

// NewCrawler creates an IPFS crawler and starts our libp2p node.
// TODO: Ping server here to see if it's running
func NewCrawler(reportInterval uint) *Crawler {
	fmt.Println("IPBW: Starting crawler")

	// Make sure server is up
	pingServer()

	// Create a context that, when passed to DHT queries, emits QueryEvents to this channel
	ctx, cancel := context.WithCancel(context.Background())
	ctx, events := routing.RegisterForQueryEvents(ctx)

	c := &Crawler{
		ctx:            ctx,
		cancel:         cancel,
		events:         events,
		reportInterval: reportInterval,
		report: &Report{
			peers: make(map[peer.ID]*Peer),
		},
	}

	c.initHost()
	return c
}

func pingServer() {
	resp, err := http.Get(ServerPing)
	if err != nil {
		fmt.Println("Expected response from server. Is it running?")
		panic(err)
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Server says: %s\n", string(body))
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

		// Get reporting peer, the IDs/Addrs they reported, and a timestamp
		reporter := report.ID
		response := filterSelf(self, report)
		timestamp := time.Now().UTC().String()

		// No responses? Skip!
		if len(response) == 0 {
			continue
		}

		c.report.mu.RLock() // Lock for reads/writes

		// Check if this reporter exists in our report yet. If not, create an entry:
		if _, exists := c.report.peers[reporter]; !exists {
			c.numReporters++
			c.report.peers[reporter] = &Peer{
				ips:       make([]string, 0), // TODO we can probably query our PeerStore here
				neighbors: make([]peer.ID, 0),
				timestamp: timestamp,
			}
		}

		// We need to update our reporter's neighbors with the IDs they just gave us.
		// First, figure out what neighbors we already know about:
		seenID := map[peer.ID]struct{}{}
		neighbors := make([]peer.ID, 0)

		// Range over already-known neighbors
		for _, neighbor := range c.report.peers[reporter].neighbors {
			// Record as neighbor and filter duplicates
			if _, seen := seenID[neighbor]; !seen {
				seenID[neighbor] = struct{}{}
				neighbors = append(neighbors, neighbor)
			}
		}

		// Next, for each newly-reported neighbor:
		// 1. Record them as one of our reporter's neighbors
		// 2. Add them to c.report.peers if they don't exist yet
		// 3. Update their report entry with the new IPs we have for them
		for _, peerInfo := range response {
			// 1. Record as neighbor and filter duplicates
			if _, seen := seenID[peerInfo.ID]; !seen {
				seenID[peerInfo.ID] = struct{}{}
				neighbors = append(neighbors, peerInfo.ID)
			}

			// 2. If we haven't seen this peer yet, create an entry for them
			if _, exists := c.report.peers[peerInfo.ID]; !exists {
				c.report.peers[peerInfo.ID] = &Peer{
					ips:       make([]string, 0),
					neighbors: []peer.ID{reporter}, // Our reporter is the first neighbor
					timestamp: timestamp,
				}
			}

			// 3. Update report entry with the new IPs we have for them
			// First, figure out what IPs we already know about:
			seenIP := map[string]struct{}{}
			knownIPs := make([]string, 0)

			// Range over already-known IPs
			for _, ip := range c.report.peers[peerInfo.ID].ips {
				// Filter duplicates
				if _, seen := seenIP[ip]; !seen {
					seenIP[ip] = struct{}{}
					knownIPs = append(knownIPs, ip)
				}
			}

			// Range over newly-reported IPs
			for _, ip := range filterIPs(peerInfo.Addrs) {
				// Filter duplicates
				if _, seen := seenIP[ip]; !seen {
					seenIP[ip] = struct{}{}
					knownIPs = append(knownIPs, ip)
				}
			}

			// Finally, add this peer's known IPs to the report:
			if len(knownIPs) != 0 {
				c.report.peers[peerInfo.ID].ips = knownIPs
			}
		}

		// Finally finally, record our reporter's neighbors
		if len(neighbors) != 0 {
			c.report.peers[reporter].neighbors = neighbors
		}

		// Unlock map for reads
		c.report.mu.RUnlock()
	}
}

// Results holds printable metrics about our crawl
type Results struct {
	reporters     uint // Number of peers that responded to our queries
	uniquePeers   uint // Number of peers that we have an ID for
	peersWithIPs  uint // Number of peers we have an IP address for
	uniqueTargets uint // Number of unique IP+port combinations we have
}

// Send c.report to server
func (c *Crawler) publishResults() (Results, string) {
	reportJSON := &ReportJSON{
		Peers: make([]PeerJSON, 0),
	}

	results := &Results{
		reporters: c.numReporters,
	}

	// This will be used to calculate the number of unique peers
	seenID := map[peer.ID]struct{}{}

	// Iterate over report and convert to ReportJSON
	c.report.mu.Lock()
	for pid, peer := range c.report.peers {

		// Add unique peer
		if _, seen := seenID[pid]; !seen {
			seenID[pid] = struct{}{}
			results.uniquePeers++
		}

		// If we have IPs for this peer, tally those
		// This branch is independent from uniqueness tally, because we may have seen
		// this peer in another peer's neighbors, but this is definitely the only place
		// we've seen this peer's IPs
		if len(peer.ips) != 0 {
			results.peersWithIPs++
			results.uniqueTargets += uint(len(peer.ips))
		}

		// Convert neighbor IDs to strings
		neighborStrings := make([]string, 0)
		for _, neighbor := range peer.neighbors {
			// While we're at it, tally unique peers:
			if _, seen := seenID[neighbor]; !seen {
				results.uniquePeers++
			}

			neighborStrings = append(neighborStrings, neighbor.Pretty())
		}

		// Convert peer to PeerJSON
		peerJSON := PeerJSON{
			Pid:       pid.Pretty(),
			Ips:       peer.ips,
			Neighbors: neighborStrings,
			Timestamp: peer.timestamp,
		}

		reportJSON.Peers = append(reportJSON.Peers, peerJSON)
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
