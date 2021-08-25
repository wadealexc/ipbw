package crawler

import (
	"context"
	"fmt"
	"os"
	"os/signal"
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

	numWorkers int64
	startTime  time.Time

	known        *types.IDMap // All unique peers we have heard about
	disconnected *types.IDMap // Peers we have disconnected from
	unreachable  *types.IDMap // Peers we were unable to connect to

	connected *types.PeerList // Peers we are currently connected to
	backlog   *types.PeerList // Peers we have not tried to connect to
}

func NewDHT() (*DHT, error) {
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
		Emitter:      events.NewEmitter(),
		host:         host,
		known:        types.NewIDMap(),
		disconnected: types.NewIDMap(),
		unreachable:  types.NewIDMap(),
		connected:    types.NewPeerList(),
		backlog:      types.NewPeerList(),
	}

	// TODO: actually handle incoming connections
	host.SetStreamHandler(types.DHT_PROTO, dht.handleIncoming)

	return dht, nil
}

func (dht *DHT) Start() {

	ctx, cancel := context.WithCancel(context.Background())

	// Record start time
	dht.startTime = time.Now()

	// Set up listeners for events during the crawl
	dht.setup(ctx)

	// Connect to bootstrap peers and start crawling!
	dht.bootstrap(ctx)

	// Create timers to stop crawl and print stats
	stopTicker := time.NewTicker(10 * time.Minute)
	statsTicker := time.NewTicker(10 * time.Second)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)

	for {
		select {
		case <-statsTicker.C:
			dht.printStats()
		case <-stopTicker.C:
			fmt.Println("Stopping crawl...")
			cancel()
			return
		case <-ch:
			fmt.Println("Stopping crawl from user interrupt...")
			cancel()
			return
		}
	}
}

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
			peer, empty := dht.backlog.Pop()
			// No peers in backlog; do nothing
			if empty {
				fmt.Printf("No peers in backlog!\n")
				return
			}

			dht.connect(ctx, peer)
		}
	}

	// Set connected to notify us when we're under MIN_CONNECTED
	dht.connected.SetNotifySize(MIN_CONNECTED)
	dht.connected.On("under-limit", maybeConnect)

	// Emitted when we add new peers to the backlog
	dht.backlog.On("new-peers", maybeConnect)

	// Emitted each time a peer tells us about new peers
	dht.On("new-peers", func(addrs []peer.AddrInfo) {

		newPeers := make([]*types.Peer, 0, len(addrs))

		for _, addr := range addrs {
			// Add peer to DHT. If we already knew about this peer,
			// we do nothing.
			added := dht.known.Add(addr.ID)
			if !added {
				continue
			}

			// New peer!
			p := types.NewPeer(addr)
			newPeers = append(newPeers, p)
		}

		// Add all new peers to backlog. We'll connect to them
		// if we drop below MIN_CONNECTED connections
		dht.backlog.AddAll(newPeers)
	})
}

// Set up listeners for a peer and attempt to connect to them
func (dht *DHT) connect(ctx context.Context, p *types.Peer) {
	// Set up callbacks for peer:

	// Fired when we successfully connect to the peer
	p.Once("connected", func() {
		// Fired if we get an error reading/writing to the peer
		p.Once("error", func(err error) {
			dht.logDCError(err)
			p.Disconnect()
		})

		// Fired when we disconnect from the peer
		p.Once("disconnected", func() {
			// Clean up:
			p.RemoveAllListeners()
			atomic.AddInt64(&dht.numWorkers, -1)
			// Remove from connected and add to disconnected
			dht.connected.Remove(p)
			dht.disconnected.Add(p.ID)
		})

		// Fired each time we read a message from this peer
		p.On("read-message", func(mType types.MessageType, mSize int, addrs []peer.AddrInfo) {
			dht.logRead(mType, mSize)
			dht.Emit("new-peers", addrs)
		})

		// Fired each time we write a message to this peer
		p.On("write-message", func(mType types.MessageType, mSize int) {
			dht.logWrite(mType, mSize)
		})

		// Add to active connections and start worker
		dht.connected.Add(p)
		go p.Worker(ctx)
	})

	// Fired if we are unable to connect to the peer
	p.Once("unreachable", func(err error) {
		// Clean up:
		p.RemoveAllListeners()
		atomic.AddInt64(&dht.numWorkers, -1)
		// Log error and add to unreachable
		dht.logConnError(err)
		dht.unreachable.Add(p.ID)
	})

	// Record new worker and try connecting
	atomic.AddInt64(&dht.numWorkers, 1)
	go p.TryConnect(dht.host)
}

func (dht *DHT) bootstrap(ctx context.Context) {

	bootstrapPeers := kadDHT.GetDefaultBootstrapPeerAddrInfos()

	numUnreachable := uint64(0)
	maxUnreachable := uint64(len(bootstrapPeers))

	// Connect to bootstrap peers
	for _, addr := range bootstrapPeers {

		p := types.NewPeer(addr)

		// Set up special listeners for our bootstrap peers:

		p.Once("connected", func() {
			fmt.Printf("Connected to bootstrap peer: %s @ %v\n", addr.ID.Pretty(), addr.Addrs)
		})

		p.Once("unreachable", func(err error) {
			fmt.Printf("Error connecting to bootstrap peer: %v\n", err)

			atomic.AddUint64(&numUnreachable, 1)
			if atomic.LoadUint64(&numUnreachable) >= maxUnreachable {
				panic("Could not connect to any bootstrap peers!")
			}
		})

		// Attempt to connect to the peer!
		dht.connect(ctx, p)
	}
}

func (dht *DHT) handleIncoming(s network.Stream) {
	// TODO
}

func (dht *DHT) workerCount() int64 {
	return atomic.LoadInt64(&dht.numWorkers)
}

func (dht *DHT) logRead(mType types.MessageType, mSize int) {
	// TODO
}

func (dht *DHT) logWrite(mType types.MessageType, mSize int) {
	// TODO
}

func (dht *DHT) logDCError(err error) {
	// TODO
}

func (dht *DHT) logConnError(err error) {
	// TODO
}

func (dht *DHT) printStats() {

	strs := []string{}

	// Header
	strs = append(strs, fmt.Sprintf("========================="))
	strs = append(strs, fmt.Sprintf("CRAWL INFO:"))
	strs = append(strs, fmt.Sprintf("========================="))

	// Duration
	timeElapsed := time.Since(dht.startTime)
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

}
