package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-datastore"
	badger "github.com/ipfs/go-ds-badger"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/libp2p/go-libp2p-core/test"
	host "github.com/libp2p/go-libp2p-host"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	peerstore "github.com/libp2p/go-libp2p-peerstore"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	"github.com/multiformats/go-multiaddr"
)

type logOutput struct {
	Id       string
	Agent    string
	Protocol string
	Addrs    []string
	Protos   []string
}

const WORKERS = 8

const PEERID_FILE = "peerIDs.csv"
const ID_TO_IP_FILE = "peerIPs.csv"

type Crawler struct {
	host host.Host // Our libp2p node
	dht  *dht.IpfsDHT
	ds   datastore.Batching
	id   *identify.IDService

	results map[peer.ID][]*peer.AddrInfo
	// Writer/File for peerIDs discovered
	idWriter *csv.Writer
	idFile   *os.File
	// Writer/File for peerID -> IP mapping
	ipWriter *csv.Writer
	ipFile   *os.File

	ctx    context.Context
	cancel context.CancelFunc
	events <-chan *routing.QueryEvent // DHT sends QueryEvents to this channel
	// Waits for a collection of goroutines to finish
	// Main goroutine (this file) calls wg.Add to set the number of goroutines to wait for
	// Then, each goroutine runs and calls Done when finished
	wg sync.WaitGroup
}

// Alias Crawler as notifee, which will implement network.Notifiee interface
type notifee Crawler

// Declare that alias'd Crawler implements network.Notifiee
var _ network.Notifiee = (*notifee)(nil)

func NewCrawler() *Crawler {
	ctx, cancel := context.WithCancel(context.Background())
	// Create a context that, when passed to DHT queries, emits QueryEvents to this channel
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

func (c *Crawler) initCSV() {
	file, err := os.Create(PEERID_FILE)
	if err != nil {
		panic(err)
	}
	writer := csv.NewWriter(file)

	c.idWriter = writer
	c.idFile = file

	file, err = os.Create(ID_TO_IP_FILE)
	if err != nil {
		panic(err)
	}
	writer = csv.NewWriter(file)

	c.ipWriter = writer
	c.ipFile = file
}

func (c *Crawler) initHost() {
	var err error
	// Create a new libp2p node
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

	// Iterate over default bootstrap peers
	for _, a := range dht.DefaultBootstrapPeers {
		// Convert multiaddr to AddrInfo
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

func (c *Crawler) close() {
	c.cancel()
	c.wg.Done()

	if err := c.ds.Close(); err != nil {
		fmt.Printf("error while shutting down: %v\n", err)
	}

	c.writeToFile()
}

func (c *Crawler) start() {
	fmt.Println("INTERPLANETARY BLACK WIDOW HAS BEGUN CRAWLING...")

	for i := 0; i < WORKERS; i++ {
		c.wg.Add(1)
		go c.worker()
	}

	go c.aggregator()
	go c.reporter()

	c.host.Network().Notify((*notifee)(c))
}

func (c *Crawler) worker() {
	fmt.Println("Starting glorious worker...")
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

func (c *Crawler) reporter() {
	fmt.Println("Starting reporter...")

	for {
		select {
		case <-time.After(10 * time.Second):
			fmt.Printf("--- %d unique peers so far\n", len(c.results))
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Crawler) aggregator() {
	fmt.Println("Starting aggregator...")

	for e := range c.events {
		// We only care about PeerResponse events, as these
		// are peers informing us of peers they know.
		if e.Type == routing.PeerResponse {

			// If we haven't seen this peer yet, we don't need to think too hard
			// Just set the responses and continue!
			if _, exists := c.results[e.ID]; !exists {
				c.results[e.ID] = e.Responses
				// fmt.Printf("New peer %s! Total unique peers: %d\n", e.ID.Pretty(), len(results))
			} else {
				// count := len(results[e.ID])

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
				for _, r := range e.Responses {
					// filter dupes
					if _, exists := seen[r.ID]; exists {
						continue
					}

					seen[r.ID] = struct{}{}
					responses = append(responses, r)
				}

				c.results[e.ID] = responses

				// newCount := len(results[e.ID])
				// fmt.Printf("Existing peer %s! Added %d new conns for a total of %d\n", e.ID.Pretty(), newCount-count, newCount)
			}
		}
	}
}

// in go:
// peer: [peerA, peerB]

// f1 list tuple(ID,ID)
// pOrigina,pDestx
// pOriginb,pDesty

// f2 map ID -> IP
// pID,pIP
func (c *Crawler) writeToFile() {
	fmt.Println("Writing results to file...")

	// Make sure we close our files and flush our writers at the end of this method
	defer c.idFile.Close()
	defer c.ipFile.Close()

	defer c.idWriter.Flush()
	defer c.ipWriter.Flush()

	pStore := c.host.Peerstore()

	for pID, addrs := range c.results {
		// Try to get IPv4/IPv6 addresses for this peer
		pInfo := pStore.Addrs(pID)

		// Maybe we don't know ANY multiaddrs for this peer? Skip if so...
		if len(pInfo) == 0 {
			fmt.Printf("No addrs returned for peer %s; skipping...\n", pID.Pretty())
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
				// Found IPv4/IPv4 or DNS; add to result
				if field == "ip4" || field == "ip6" || field == "dns4" || field == "dns6" {
					if res != "" {
						fmt.Printf("Possible malformed ip %s; skipping...\n", a.String())
						break
					} else if i+1 >= len(fields) {
						fmt.Printf("Unexpected EOField %s; skipping...\n", a.String())
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
				c.ipWriter.Write(result)
			}
		}

		// So, we found multiaddrs for this peer, but didn't find IPv4 / IPv6? Weird... skip.
		if count == 0 {
			fmt.Printf("%s has %d maddrs, but no ip4/ip6!\nAddrs:%v\n", pID.Pretty(), len(pInfo), pInfo)
			continue
		}

		for _, addr := range addrs {
			result := make([]string, 2)
			result[0] = pID.Pretty()
			result[1] = addr.ID.Pretty()
			c.idWriter.Write(result)
		}
	}
}

func (n *notifee) Connected(net network.Network, conn network.Conn) {
	p := conn.RemotePeer()
	pstore := net.Peerstore()

	go func() {
		// Ask the IDService about the connection
		n.id.IdentifyConn(conn)
		<-n.id.IdentifyWait(conn)

		// Get the protocols supported by the peer
		protos, err := pstore.GetProtocols(p)
		if err != nil {
			panic(err)
		}

		// Get all known / valid addresses for the peer
		addrs := pstore.Addrs(p)

		// Get peer's agent version
		agent, err := pstore.Get(p, "AgentVersion")
		switch err {
		case nil:
		case peerstore.ErrNotFound:
			agent = ""
		default:
			panic(err)
		}

		// Get peer's protocol version
		protocol, err := pstore.Get(p, "ProtocolVersion")
		switch err {
		case nil:
		case peerstore.ErrNotFound:
			protocol = ""
		default:
			panic(err)
		}

		line := logOutput{
			Id:       p.Pretty(),
			Agent:    agent.(string),
			Protocol: protocol.(string),
			Addrs: func() (ret []string) {
				for _, a := range addrs {
					ret = append(ret, a.String())
				}
				return ret
			}(),
			Protos: protos,
		}

		_, err = json.Marshal(line)
		if err != nil {
			panic(err)
		}
	}()
}

func (*notifee) Disconnected(_ network.Network, conn network.Conn) {
	// fmt.Printf("Disconnected from peer: %s\n", conn.RemotePeer())
}

func (*notifee) Listen(_ network.Network, maddr multiaddr.Multiaddr) {
	// fmt.Printf("Started listening on address: %s\n", maddr.String())
}

func (*notifee) ListenClose(_ network.Network, maddr multiaddr.Multiaddr) {
	// fmt.Printf("Stopped listening on address: %s\n", maddr.String())
}

func (*notifee) OpenedStream(_ network.Network, st network.Stream) {
	// fmt.Printf("Opened stream with ID: %s\n", st.ID())
}

func (*notifee) ClosedStream(_ network.Network, st network.Stream) {
	// fmt.Printf("Closed stream with ID: %s\n", st.ID())
}

func main() {
	crawler := NewCrawler()

	crawler.start()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)

	select {
	case <-time.After(60 * time.Second):
		fmt.Println("60 seconds reached; stopping crawler...")
		crawler.close()
		return
	case <-ch:
		crawler.close()
		return
	case <-crawler.ctx.Done():
		return
	}
}
