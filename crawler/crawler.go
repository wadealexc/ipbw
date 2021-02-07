package crawler

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-datastore"
	badger "github.com/ipfs/go-ds-badger"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	host "github.com/libp2p/go-libp2p-host"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
)

// Version of the crawler instance
const Version = "0.1"

// Crawler holds info about an active crawl
type Crawler struct {
	// IPFS / libp2p fields
	host host.Host
	dht  *dht.IpfsDHT
	ds   datastore.Batching
	id   *identify.IDService

	// We receive QueryEvents from the DHT on this channel
	events <-chan *routing.QueryEvent
	report *Report // Result aggregator

	// Context and cancel function passed to DHT queries
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	opts *Options
}

// Report aggregates results from QueryEvents
type Report struct {
	mu    sync.RWMutex
	peers map[peer.ID]*Peer
}

// Peer contains all the info we know for a given peer
type Peer struct {
	isReporter bool      // Whether this peer reported other peers to us
	ips        []string  // All known IPs/ports for this peer
	neighbors  []peer.ID // All neighbors this peer reported to us
	timestamp  string    // The UTC timestamp when we discovered the peer
}

// Options are various configurations for the crawler
type Options struct {
	NoPublish       bool   // Whether we're publishing reports to a server
	PublishInterval uint   // How often a report is sent to our server
	ReportInterval  uint   // How often a report will be printed to the console
	NumWorkers      uint   // Number of goroutines to query the DHT with
	Server          string // Endpoint to publish results to
	ServerPing      string // Endpoint to check if server is running
	APIKey          string // API key to authenticate with to the report server
}

// NewCrawler starts our libp2p node and bootstraps connections
func NewCrawler(ctx context.Context, opts *Options) (*Crawler, error) {

	// Create a context that, when passed to DHT queries, emits QueryEvents to a channel
	ctx, cancel := context.WithCancel(ctx)
	ctx, events := routing.RegisterForQueryEvents(ctx)

	// Start a libp2p node
	host, err := libp2p.New(context.Background())
	if err != nil {
		return nil, err
	}

	// Create a DB for our DHT client
	ds, err := badger.NewDatastore("dht.db", nil)
	if err != nil {
		return nil, err
	}

	crawler := &Crawler{
		ctx:    ctx,
		cancel: cancel,
		events: events,
		host:   host,
		dht:    dht.NewDHTClient(ctx, host, ds),
		ds:     ds,
		id:     identify.NewIDService(host),
		report: &Report{
			peers: make(map[peer.ID]*Peer),
		},
		opts: opts,
	}

	// Bootstrap
	for _, a := range dht.DefaultBootstrapPeers {
		pInfo, err := peer.AddrInfoFromP2pAddr(a)
		if err != nil {
			return nil, err
		}

		ctx, cancel = context.WithTimeout(crawler.ctx, 5*time.Second)
		if err = crawler.host.Connect(ctx, *pInfo); err != nil {
			fmt.Printf("skipping bootstrap peer: %s\n", pInfo.ID.Pretty())
		}

		cancel()
	}

	// Make sure server is running
	if !crawler.opts.NoPublish {
		response, err := crawler.pingServer()
		if err != nil {
			return nil, err
		}

		fmt.Printf("Server says: %s\n", response)
	}

	return crawler, nil
}

// Start spawns all the goroutines we need for world domination
func (c *Crawler) Start() {
	for i := 0; i < int(c.opts.NumWorkers); i++ {
		c.wg.Add(1)
		go c.worker() // Queries DHT
	}

	go c.aggregator() // Collects results from c.events
	go c.logger()     // Prints intermittent status updates to console
	if !c.opts.NoPublish {
		go c.reporter() // Publishes results to server
	}
}

// Stop stops the crawl, but waits briefly to allow tasks to finish
func (c *Crawler) Stop() {
	// 5 seconds is the empirically-derived correct time to wait
	waitTime := time.Duration(5) * time.Second

	select {
	case <-time.After(waitTime):
		c.Kill()
		return
	}
}

// Kill stops the crawl ASAP
func (c *Crawler) Kill() {
	if err := c.ds.Close(); err != nil {
		fmt.Printf("error while shutting down: %v\n", err)
	}

	c.cancel()
	c.wg.Done()
}

// Each worker generates a random peer ID, then queries the DHT
// Results are received via the c.events channel
func (c *Crawler) worker() {
Work:
	id, err := randPeerID()
	if err != nil {
		fmt.Printf("Error getting random peer ID: %v\n", err)
		goto Work
	}

	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	_, _ = c.dht.FindPeer(ctx, id)
	cancel()
	goto Work
}

// Publishes reports to a server
func (c *Crawler) reporter() {
	publishInterval := time.Duration(c.opts.PublishInterval) * time.Minute
	for {
		select {
		case <-time.After(publishInterval):
			if c.opts.NoPublish {
				continue
			}

			// Send report to server
			response, err := c.publishResults()
			if err != nil {
				fmt.Printf("Error publishing result: %v\n", err)
			} else {
				fmt.Printf("Sent report to (%s) and got response: %s\n", c.opts.Server, response)
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// Prints incremental status updates to console
func (c *Crawler) logger() {
	startTime := time.Now()
	reportInterval := time.Duration(c.opts.ReportInterval) * time.Minute
	for {
		select {
		case <-time.After(reportInterval):
			timeElapsed := uint(time.Now().Sub(startTime).Minutes())

			// Tally results and print
			results := c.getResults()

			// VERY important to get this right or the whole world will burn
			if timeElapsed == 1 {
				fmt.Printf("--- Time elapsed: %d minute ---\n", timeElapsed)
			} else {
				fmt.Printf("--- Time elapsed: %d minutes ---\n", timeElapsed)
			}

			fmt.Printf(">> %d peers reporting:\n", results.numReporters)
			fmt.Printf("- %d unique peer IDs -\n", results.uniquePeers)
			fmt.Printf("- %d peers with a valid IP+port -\n", results.peersWithIPs)
			fmt.Printf("- %d unique IP+port combinations -\n", results.uniqueTargets)
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

		c.report.mu.RLock() // Lock for reads

		// Check if this reporter exists in our report yet. If not, create an entry:
		if _, exists := c.report.peers[reporter]; !exists {
			c.report.peers[reporter] = &Peer{
				isReporter: true,
				ips:        make([]string, 0), // TODO we can probably query our PeerStore here
				neighbors:  make([]peer.ID, 0),
				timestamp:  timestamp,
			}
		} else if !c.report.peers[reporter].isReporter {
			// They exist, but have not been a reporter yet
			c.report.peers[reporter].isReporter = true
		}

		// We need to update our reporter's neighbors with the IDs they just gave us.
		// First, figure out what neighbors we already know about:
		seenID := map[peer.ID]struct{}{}
		neighbors := make([]peer.ID, 0)
		seenID[reporter] = struct{}{} // No, you aren't your own neighbor

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

// Convert current report to JSON and send to server
func (c *Crawler) publishResults() (string, error) {
	reportJSON := &ReportJSON{
		Peers: make([]PeerJSON, 0),
	}

	c.report.mu.Lock() // Lock report for writes so we can read

	for id, peer := range c.report.peers {
		// Convert neighbor IDs to strings
		neighborStrings := make([]string, len(peer.neighbors))
		for _, neighbor := range peer.neighbors {
			neighborStrings = append(neighborStrings, neighbor.Pretty())
		}

		// Convert peer to PeerJSON
		peerJSON := PeerJSON{
			Pid:       id.Pretty(),
			Ips:       peer.ips,
			Neighbors: neighborStrings,
			Timestamp: peer.timestamp,
		}

		reportJSON.Peers = append(reportJSON.Peers, peerJSON)
	}
	c.report.mu.Unlock()

	reportBody, err := json.Marshal(reportJSON)
	if err != nil {
		return "", err
	}

	client := &http.Client{}

	req, err := http.NewRequest("POST", c.opts.Server, bytes.NewBuffer(reportBody))
	if err != nil {
		return "", err
	}

	req.Header.Add("User-Agent", fmt.Sprintf("ipbw-go-%s", Version))
	req.Header.Add("Authorization", c.opts.APIKey)

	resp, err := client.Do(req)

	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

// Results holds printable metrics about our crawl
type Results struct {
	numReporters  uint // Number of peers that responded to our queries
	uniquePeers   uint // Number of peers that we have an ID for
	peersWithIPs  uint // Number of peers we have an IP address for
	uniqueTargets uint // Number of unique IP+port combinations we have
}

// Iterates over our report and retrieves printable metrics
func (c *Crawler) getResults() Results {

	c.report.mu.Lock()
	defer c.report.mu.Unlock()

	results := &Results{
		uniquePeers: uint(len(c.report.peers)),
	}

	// Track which IPs we've seen for unique target tally
	seenIP := map[string]struct{}{}

	for _, peer := range c.report.peers {
		if peer.isReporter {
			results.numReporters++
		}

		if len(peer.ips) != 0 {
			results.peersWithIPs++
		}

		for _, ip := range peer.ips {
			if _, seen := seenIP[ip]; !seen {
				results.uniqueTargets++
				seenIP[ip] = struct{}{}
			}
		}
	}

	return *results
}

func (c *Crawler) pingServer() (string, error) {
	client := &http.Client{}

	req, err := http.NewRequest("GET", c.opts.ServerPing, bytes.NewBuffer(nil))
	if err != nil {
		return "", err
	}

	req.Header.Add("User-Agent", fmt.Sprintf("ipbw-go-%s", Version))
	req.Header.Add("Authorization", c.opts.APIKey)

	resp, err := client.Do(req)

	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
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

// Iterates over a collection of multiaddrs and attempts to find ip+port combos
func filterIPs(addrs []ma.Multiaddr) []string {
	var results = make([]string, 0)
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
			if field == "ip4" || field == "ip6" || field == "dns" || field == "dns4" || field == "dns6" || field == "dnsaddr" { // found ip
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
				if field == "dns" || field == "dns4" || field == "dns6" || field == "dnsaddr" {
					res = ipRaw
					ipFound = true
					break
				}

				// For IPv4 / IPv6 addresses, filter out private / local / loopback IPs
				ip := net.ParseIP(ipRaw)
				switch {
				case ip == nil:
					fmt.Printf("Invalid IP address in %s: %s; skipping\n", addr.String(), ipRaw)
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

func randPeerID() (peer.ID, error) {
	buf := make([]byte, 16)
	rand.Read(buf)
	h, err := mh.Sum(buf, mh.SHA2_256, -1)
	if err != nil {
		return "", err
	}
	return peer.ID(h), nil
}
