package crawler

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/ipfs/go-datastore"
	badger "github.com/ipfs/go-ds-badger"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/routing"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
	"go.uber.org/fx"
)

// Crawler contains all the infrastructure needed to crawl the IPFS DHT
type Crawler struct {
	host.Host
	DHT *dht.IpfsDHT
	DS  datastore.Batching
	ID  *identify.IDService
	PS  peerstore.Peerstore

	// We receive QueryEvents from the DHT on this channel
	Events    <-chan *routing.QueryEvent
	DHTCtx    context.Context
	DHTCancel context.CancelFunc

	WorkerCtx    context.Context
	WorkerCancel context.CancelFunc

	*Report
	NumWorkers uint
}

type crawlerParams struct {
	fx.In
}

// NewCrawler instantiates our libp2p node and dht database
func NewCrawler(params crawlerParams, lc fx.Lifecycle) (*Crawler, error) {

	// Create a context that, when passed to DHT queries, emits QueryEvents to a channel
	DHTCtx, DHTCancel := context.WithCancel(context.Background())
	DHTCtx, events := routing.RegisterForQueryEvents(DHTCtx)

	// Create context/cancel for our workers
	WorkerCtx, WorkerCancel := context.WithCancel(context.Background())

	crawler := &Crawler{
		Report: &Report{
			Peers: make(map[peer.ID]*Peer),
		},
		DHTCtx:       DHTCtx,
		DHTCancel:    DHTCancel,
		Events:       events,
		WorkerCtx:    WorkerCtx,
		WorkerCancel: WorkerCancel,
	}

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			if err := crawler.build(); err != nil {
				return fmt.Errorf("Error building crawler: %v", err)
			}
			return crawler.start()
		},
		OnStop: func(context.Context) error {
			return crawler.stop()
		},
	})

	return crawler, nil
}

// SetNumWorkers sets the number of processes that will be launched to query the DHT
func (c *Crawler) SetNumWorkers(numWorkers uint) error {
	if numWorkers == 0 {
		return fmt.Errorf("Cannot query DHT without workers")
	}
	c.NumWorkers = numWorkers
	return nil
}

// Build the crawler: start libp2p node and open a DB for our DHT
func (c *Crawler) build() error {

	if c.NumWorkers == 0 {
		return fmt.Errorf("Expected nonzero number of workers")
	}

	// Start a libp2p node
	host, err := libp2p.New(context.Background())
	if err != nil {
		return fmt.Errorf("Error creating libp2p node: %v", err)
	}
	c.Host = host

	// Create a DB for our DHT client
	ds, err := badger.NewDatastore("dht.db", nil)
	if err != nil {
		return fmt.Errorf("Error getting datastore: %v", err)
	}
	c.DS = ds

	c.DHT = dht.NewDHTClient(context.Background(), host, ds)
	c.ID = identify.NewIDService(host)
	c.PS = host.Peerstore()

	// Bootstrap peers
	for _, a := range dht.DefaultBootstrapPeers {
		pInfo, err := peer.AddrInfoFromP2pAddr(a)
		if err != nil {
			return fmt.Errorf("Error converting bootstrap address: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err = c.Host.Connect(ctx, *pInfo); err != nil {
			fmt.Printf("skipping bootstrap peer: %s\n", pInfo.ID.Pretty())
		}

		cancel()
	}

	return nil
}

// Start crawler's long-running processes:
// - workers to query the DHT
// - an aggregator to collect results from worker queries
func (c *Crawler) start() error {
	if c.NumWorkers == 0 {
		return fmt.Errorf("Expected nonzero NumWorkers")
	}

	for i := 0; i < int(c.NumWorkers); i++ {
		go c.spawnWorker()
	}

	go c.aggregator()
	return nil
}

func (c *Crawler) stop() error {
	fmt.Printf("Stopping crawler...\n")

	c.DHTCancel()
	c.WorkerCancel()

	// Wait briefly to give the crawler a chance to shut down
	// 5 seconds is the empirically-derived correct amount of time to wait:
	waitFor := time.Duration(5) * time.Second
	select {
	case <-time.After(waitFor):
		return nil
	}
}

func (c *Crawler) spawnWorker() {
Work:
	// Check if we've been told to stop
	if c.WorkerCtx.Err() != nil {
		return
	}
	id, err := randPeerID()
	if err != nil {
		fmt.Printf("Error getting random peer ID: %v\n", err)
		goto Work
	}

	ctx, cancel := context.WithTimeout(c.DHTCtx, 10*time.Second)
	_, _ = c.DHT.FindPeer(ctx, id)
	cancel()
	goto Work
}

// Collects results from DHT QueryEvents into our Report
func (c *Crawler) aggregator() {
	// Get our PeerID, so we can filter responses that come from our own node
	self := c.DHT.PeerID()

	for report := range c.Events {

		// Check if we've been told to stop
		if c.WorkerCtx.Err() != nil {
			return
		}

		if report.Type == routing.QueryError {
			// fmt.Printf("Got QueryError: %s\n", report.Extra)
			continue
		} else if report.Type != routing.PeerResponse {
			continue
		}

		// Get reporting peer, the IDs/Addrs they reported, and a timestamp
		reporter := report.ID
		response := c.filterSelf(self, report)
		timestamp := time.Now().UTC().String()

		// No responses? Skip!
		if len(response) == 0 {
			continue
		}

		c.Report.Mu.RLock() // Lock for reads

		// Check if this reporter exists in our report yet. If not, create an entry:
		if _, exists := c.Report.Peers[reporter]; !exists {
			c.Report.Peers[reporter] = &Peer{
				IsReporter: true,
				Ips:        make([]string, 0), // TODO we can probably query our PeerStore here
				Neighbors:  make([]peer.ID, 0),
				Timestamp:  timestamp,
			}
		} else if !c.Report.Peers[reporter].IsReporter {
			// They exist, but have not been a reporter yet
			c.Report.Peers[reporter].IsReporter = true
		}

		// We need to update our reporter's neighbors with the IDs they just gave us.
		// First, figure out what neighbors we already know about:
		seenID := map[peer.ID]struct{}{}
		neighbors := make([]peer.ID, 0)
		seenID[reporter] = struct{}{} // No, you aren't your own neighbor

		// Range over already-known neighbors
		for _, neighbor := range c.Report.Peers[reporter].Neighbors {
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
			if _, exists := c.Report.Peers[peerInfo.ID]; !exists {
				c.Report.Peers[peerInfo.ID] = &Peer{
					Ips:       make([]string, 0),
					Neighbors: []peer.ID{reporter}, // Our reporter is the first neighbor
					Timestamp: timestamp,
				}
			}

			// 3. Update report entry with the new IPs we have for them
			// First, figure out what IPs we already know about:
			seenIP := map[string]struct{}{}
			knownIPs := make([]string, 0)

			// Range over already-known IPs
			for _, ip := range c.Report.Peers[peerInfo.ID].Ips {
				// Filter duplicates
				if _, seen := seenIP[ip]; !seen {
					seenIP[ip] = struct{}{}
					knownIPs = append(knownIPs, ip)
				}
			}

			// Range over newly-reported IPs
			for _, ip := range c.filterIPs(peerInfo.Addrs) {
				// Filter duplicates
				if _, seen := seenIP[ip]; !seen {
					seenIP[ip] = struct{}{}
					knownIPs = append(knownIPs, ip)
				}
			}

			// Finally, add this peer's known IPs to the report:
			if len(knownIPs) != 0 {
				c.Report.Peers[peerInfo.ID].Ips = knownIPs
			}
		}

		// Finally finally, record our reporter's neighbors
		if len(neighbors) != 0 {
			c.Report.Peers[reporter].Neighbors = neighbors
		}

		// Unlock map for reads
		c.Report.Mu.RUnlock()
	}
}

// Filter DHT query responses that contain our own peer ID
func (c *Crawler) filterSelf(self peer.ID, report *routing.QueryEvent) []*peer.AddrInfo {
	var res []*peer.AddrInfo = make([]*peer.AddrInfo, 0)

	// We don't want reports from our own node
	if report.ID == self {
		fmt.Printf("Unexpected self-report!\n")
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
func (c *Crawler) filterIPs(addrs []ma.Multiaddr) []string {
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

// Generates a random peer ID
func randPeerID() (peer.ID, error) {
	buf := make([]byte, 16)
	rand.Read(buf)
	h, err := mh.Sum(buf, mh.SHA2_256, -1)
	if err != nil {
		return "", err
	}
	return peer.ID(h), nil
}
