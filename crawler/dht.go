package crawler

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	crypto "github.com/libp2p/go-libp2p-crypto"
	kadDHT "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/wadeAlexC/go-events/events"
	"github.com/wadeAlexC/ipbw/crawler/types"
)

// MAX_WORKERS is the maximum number of workers we allow
const MAX_WORKERS = 1500

// MIN_CONNECTED is the minimum number of connections we
// can have before trying to connect to peers in the backlog
const MIN_CONNECTED = 500

const PORT = 1337
const USER_AGENT = "IPBW"

type DHT struct {
	*events.Emitter
	host host.Host

	crawlDuration uint

	numWorkers int64

	known        *types.IDMap // All unique peers we have heard about
	disconnected *types.IDMap // Peers we have disconnected from
	unreachable  *types.IDMap // Peers we were unable to connect to

	connected *types.PeerList // Peers we are currently connected to
	backlog   *types.PeerList // Peers we have not tried to connect to

	stats *DHTStats
}

func NewDHT(duration uint) (*DHT, error) {
	// Generate a new identity
	pk, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	if err != nil {
		return nil, fmt.Errorf("error generating key pair: %v", err)
	}

	// Create a new libp2p Node
	host, err := libp2p.New(
		context.Background(),
		libp2p.Identity(pk),
		libp2p.UserAgent(USER_AGENT),
	)
	if err != nil {
		return nil, fmt.Errorf("error creating libp2p node: %v", err)
	}

	dht := &DHT{
		Emitter:       events.NewEmitter(),
		host:          host,
		crawlDuration: duration,
		known:         types.NewIDMap(),
		disconnected:  types.NewIDMap(),
		unreachable:   types.NewIDMap(),
		connected:     types.NewPeerList(MIN_CONNECTED),
		backlog:       types.NewPeerList(0),
		stats:         NewDHTStats(),
	}

	// Handler for incoming connections from peers
	host.SetStreamHandler(types.DHT_PROTO, dht.handleIncoming)

	return dht, nil
}

// Start does three important things:
// 1. Sets up listeners to manage workload during the crawl
// 2. Connects to bootstrap peers and begins crawling
// 3. Prints stats at regular intervals
func (dht *DHT) Start() {

	ctx, cancel := context.WithCancel(context.Background())

	// Record start time
	dht.stats.setStartTime()

	// Set up listeners for events during the crawl
	dht.setup(ctx)

	// Connect to bootstrap peers and start crawling!
	dht.bootstrap(ctx)

	// Create a timer to print stats
	endless := dht.crawlDuration == 0
	statsTicker := time.NewTicker(10 * time.Second)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)

	if endless {
		fmt.Printf("Crawling until told to stop\n")
		for {
			select {
			case <-statsTicker.C:
				dht.printCrawlStatus(getHeader("CRAWL INFO"))
			case <-ch:
				fmt.Println("Stopping crawl from user interrupt...")
				cancel()
				dht.EmitSync("stop")
				return
			}
		}
	} else {
		fmt.Printf("Crawling for %d minutes\n", dht.crawlDuration)
		stopTicker := time.NewTicker(time.Duration(dht.crawlDuration) * time.Minute)

		for {
			select {
			case <-statsTicker.C:
				dht.printCrawlStatus(getHeader("CRAWL INFO"))
			case <-stopTicker.C:
				cancel()
				dht.EmitSync("stop")
				return
			case <-ch:
				fmt.Println("Stopping crawl from user interrupt...")
				cancel()
				dht.EmitSync("stop")
				return
			}
		}
	}
}

// Set up listeners to manage workload during the crawl
func (dht *DHT) setup(ctx context.Context) {

	// This function will be called when our connected peers
	// drop under some minimum, or when we add peers to the
	// backlog.
	// It attempts to connect to peers until it has reached
	// the maximum number of workers.
	// NOTE: if we spin through a bunch of peers and all are
	// unreachable, neither connected nor backlog will trigger
	// maybeConnect to try again. Though, this should be rare.
	maybeConnect := func() {
		// Pull from backlog until we've reached MAX_WORKERS
		for dht.workerCount() < MAX_WORKERS {
			peer, popped := dht.backlog.Pop()
			// No peers in backlog; do nothing
			if !popped {
				return
			}

			dht.connect(ctx, peer)
		}
	}

	// Emitted on adds/removes if connected has fewer than
	// MIN_CONNECTED peers
	dht.connected.On("under-limit", maybeConnect)

	// Emitted when we add new peers to the backlog
	dht.backlog.On("new-peers", maybeConnect)

	// Emitted each time a peer tells us about new peers
	dht.On("new-peers", func(addrs []peer.AddrInfo) {
		newPeers := make([]*types.Peer, 0, len(addrs))

		for _, addr := range addrs {
			// Add peer to known. If we already knew about this peer,
			// we do nothing.
			added := dht.known.Add(addr.ID)
			if !added {
				continue
			}

			// New peer!
			p := types.NewPeer(addr)
			newPeers = append(newPeers, p)
		}

		// Add all new peers to backlog. We'll connect to them:
		// 1. If we drop below MIN_CONNECTED connections
		// 2. If we have fewer than MAX_WORKERS workers
		dht.backlog.AddAll(newPeers)
	})

	// Emitted when the crawl is being stopped
	dht.Once("stop", func() {
		fmt.Printf("Stopping crawl...\n")

		start := time.Now()

		// Force all workers to disconnect from their peers, and remove
		// them from connected
		numRemoved := dht.connected.ForEach(func(p *types.Peer) (remove bool) {
			p.RemoveAllListeners()
			p.HangUp()
			dht.disconnected.Add(p.ID)
			atomic.AddInt64(&dht.numWorkers, -1)
			return true
		})

		elapsed := time.Since(start).Milliseconds()
		fmt.Printf("Disconnected from %d peers in %d ms\n", numRemoved, elapsed)

		// Print final status w/ disconnect info
		header := getHeader("FINAL CRAWL STATUS")
		dht.printCrawlStatus(header)

		// Print stats from the crawl
		dht.stats.printStats()
	})
}

// Connect to each bootstrap peer and begin asking them
// for peers.
//
// If we're unable to connect to any bootstrap peers, this
// method panics.
func (dht *DHT) bootstrap(ctx context.Context) {

	bootstrapPeers := kadDHT.GetDefaultBootstrapPeerAddrInfos()

	numUnreachable := uint64(0)
	maxUnreachable := uint64(len(bootstrapPeers))

	for _, addr := range bootstrapPeers {
		// Add peer as "known"
		dht.known.Add(addr.ID)

		p := types.NewPeer(addr)

		// Set up special listeners for our bootstrap peers:
		p.Once("connected", func() {
			fmt.Printf("Connected to bootstrap peer: %s @ %v\n", addr.ID.Pretty(), addr.Addrs)
		})

		// Emitted if we were unable to reach a bootstrap peer
		p.Once("unreachable", func(err error) {
			fmt.Printf("Error connecting to bootstrap peer %s @ %v: %v\n", addr.ID.Pretty(), addr.Addrs, err)

			atomic.AddUint64(&numUnreachable, 1)
			if atomic.LoadUint64(&numUnreachable) >= maxUnreachable {
				panic("Could not connect to any bootstrap peers!")
			}
		})

		// Attempt to connect to the peer!
		dht.connect(ctx, p)
	}
}

// Initiate an outbound connection to a peer:
// 1. Set up callbacks to listen to reads / writes / etc for this peer
// 2. Attempt to connect
func (dht *DHT) connect(ctx context.Context, p *types.Peer) {
	// Record new worker
	atomic.AddInt64(&dht.numWorkers, 1)

	// Set up callbacks to listen for events from peer:
	dht.addListeners(ctx, p)

	// Try connecting to peer
	go p.TryConnect(dht.host)
}

// Called by libp2p when a peer connects to us using the
// DHT protocol handler. If we don't already know the peer,
// we start a worker for them.
//
// NOTE: We could do other things here to manage inbound connections
// from peers we already know, but the most likely scenario is
// that we already have a worker for them. So, let's not
// complicate the matter!
func (dht *DHT) handleIncoming(stream network.Stream) {
	// Get info on remote peer connecting to us
	pid := stream.Conn().RemotePeer()
	addrs := dht.host.Peerstore().Addrs(pid)
	addrInfo := peer.AddrInfo{
		ID:    pid,
		Addrs: addrs,
	}

	p := types.NewPeer(addrInfo)

	// Try to add peer to known. If we successfully added them,
	// we can start a worker immediately.
	if added := dht.known.Add(p.ID); !added {
		return
	}

	// Record new worker
	atomic.AddInt64(&dht.numWorkers, 1)
	dht.connected.Add(p)

	// Set up callbacks to listen for events from peer:
	dht.addListeners(context.Background(), p)

	// Give peer the stream and emit connected -
	// listeners will start work.
	p.SetStream(stream)
	p.Emit("connected")

	dht.stats.logInboundConn(p)
}

func (dht *DHT) addListeners(ctx context.Context, p *types.Peer) {
	// Fired when we successfully connect to the peer
	p.Once("connected", func() {

		// Auto-disconnect after 5 minutes
		pCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)

		// Fired if we get an error reading/writing to the peer
		p.Once("error", func(err error) {
			dht.stats.logError(p, err)
			p.Disconnect()
		})

		// Fired when we disconnect from the peer
		p.Once("disconnected", func() {
			// Clean up:
			cancel()
			p.RemoveAllListeners()
			// Remove from connected and add to disconnected
			dht.disconnected.Add(p.ID)
			if removed := dht.connected.Remove(p); removed {
				// Only decrease numWorkers if we removed from connected
				// That way a racing condition won't mess with this value.
				atomic.AddInt64(&dht.numWorkers, -1)
			}
		})

		// Fired once we've identified metadata about this peer
		// Metadata comes from the libp2p ID protocol
		p.Once("identified", func(protos []string, protoVersion string, agent string) {
			dht.stats.logIdentify(p, protos, protoVersion, agent)
		})

		// Fired each time we read a message from this peer
		p.On("read-message", func(mType types.MessageType, mSize int, addrs []peer.AddrInfo) {
			dht.Emit("new-peers", addrs)
			dht.stats.logRead(p, mType, mSize)
		})

		// Fired each time we write a message to this peer
		p.On("write-message", func(mType types.MessageType, mSize int) {
			dht.stats.logWrite(p, mType, mSize)
		})

		// Add to active connections and start worker
		dht.connected.Add(p)
		go p.Worker(pCtx, dht.host.Peerstore())
	})

	// Fired if we are unable to connect to the peer
	p.Once("unreachable", func(err error) {
		// Clean up:
		p.RemoveAllListeners()
		atomic.AddInt64(&dht.numWorkers, -1)
		// Log error and add to unreachable
		dht.stats.logUnreachable(p, err)
		dht.unreachable.Add(p.ID)
	})
}

func (dht *DHT) workerCount() int64 {
	return atomic.LoadInt64(&dht.numWorkers)
}

func (dht *DHT) printCrawlStatus(strs []string) {
	// Duration
	timeElapsed := dht.stats.getTimeElapsed()
	hours := uint64(timeElapsed.Hours())
	minutes := uint64(timeElapsed.Minutes()) % 60
	seconds := uint64(timeElapsed.Seconds()) % 60
	strs = append(strs, fmt.Sprintf("Time elapsed: %d hr | %d min | %d sec", hours, minutes, seconds))

	// Basic info
	strs = append(strs, fmt.Sprintf("Unique peers discovered: %d", dht.known.Count()))
	strs = append(strs, fmt.Sprintf("Current connections: %d", dht.connected.Count()))
	strs = append(strs, fmt.Sprintf("Peers in backlog: %d", dht.backlog.Count()))
	strs = append(strs, fmt.Sprintf("Total disconnected: %d", dht.disconnected.Count()))
	strs = append(strs, fmt.Sprintf("Total unreachable: %d", dht.unreachable.Count()))
	strs = append(strs, fmt.Sprintf("Number of active workers: %d", dht.workerCount()))

	strs = append(strs, getFooter())
	fmt.Println(strings.Join(strs, "\n"))
}

func getHeader(header string) []string {
	strs := []string{}

	strs = append(strs, fmt.Sprintf("========================="))
	strs = append(strs, fmt.Sprintf("%s:", header))
	strs = append(strs, fmt.Sprintf("========================="))

	return strs
}

func getFooter() string {
	return "========================="
}
