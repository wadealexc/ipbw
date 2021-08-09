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
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	kadDHT "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-peerstore/pstoremem"
)

type DHT struct {
	peerstore peerstore.Peerstore
	host      host.Host

	// Tracks all the peers we've heard about, connected to,
	// and failed to connect to
	tracker *PeerTracker

	// metrics *DHTMetrics
}

const PORT = 1337
const USER_AGENT = "IPBW"

const DHT_PROTO = "/ipfs/kad/1.0.0"
const MAX_ACTIVE_CONNS = 1000

// Maximum number of errors reading a peer's messages before we
// disconnect from them
const MAX_READ_ERRORS = 5

func NewDHT() (*DHT, error) {
	peerstore, host, err := createHost()
	if err != nil {
		return nil, fmt.Errorf("error in createHost: %v", err)
	}

	peerTracker, err := NewPeerTracker(host)
	if err != nil {
		return nil, fmt.Errorf("error creating PeerTracker: %v", err)
	}

	err = peerTracker.AddSelf(host.ID())
	if err != nil {
		return nil, fmt.Errorf("error adding self to peertracker: %v", err)
	}

	dht := &DHT{
		peerstore: peerstore,
		host:      host,
		tracker:   peerTracker,
		// metrics:   NewDHTMetrics(), // TODO
	}

	// kad := kadDHT.NewDHTClient(nil, nil, nil)
	// kad.GetValue(nil, "", nil)
	// kad.PutValue(nil, "", nil)
	// kad.Ping(nil, "")
	// kad.FindPeer(nil, "")

	// Set stream handler for peers that connect to us using the DHT protocol
	host.SetStreamHandler(DHT_PROTO, dht.handleIncomingConn)

	// TODO - generate a bunch of hosts
	// TODO - consider removing ALL existing handlers from the host

	return dht, nil
}

func createHost() (peerstore.Peerstore, host.Host, error) {
	pk, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	if err != nil {
		return nil, nil, fmt.Errorf("error generating key pair: %v", err)
	}

	peerstore := pstoremem.NewPeerstore()

	// Create a new libp2p Node
	host, err := libp2p.New(
		context.Background(),
		libp2p.Identity(pk),
		libp2p.Peerstore(peerstore),
		libp2p.UserAgent(USER_AGENT),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating libp2p node: %v", err)
	}

	return peerstore, host, nil
}

func (dht *DHT) Start(ctx context.Context) error {

	fmt.Printf("Starting DHT...\n")
	fmt.Printf("Listening on addrs: %v\n", dht.host.Addrs())
	fmt.Printf("Using protocols: %v\n", dht.host.Mux().Protocols())

	ctx, cancel := context.WithCancel(ctx)

	bootstrapSuccess := false
	// Connect to bootstrap peers
	for _, addr := range kadDHT.GetDefaultBootstrapPeerAddrInfos() {
		err := dht.tracker.BootstrapFrom(addr)
		if err == nil {
			fmt.Printf("Connected to bootstrap peer: %s @ %v\n", addr.ID.Pretty(), addr.Addrs)
			bootstrapSuccess = true
		} else {
			fmt.Printf("Error connecting to bootstrap peer: %v\n", err)
		}
	}

	if !bootstrapSuccess {
		cancel()
		return fmt.Errorf("Could not connect to any bootstrap peers; shutting down")
	}

	// Start crawler main loop
	go dht.connManager(ctx)

	// Create timers to stop the crawl and print crawl stats
	stopCrawl := time.NewTicker(10 * time.Minute)
	printStats := time.NewTicker(10 * time.Second)

	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)

	for {
		select {
		case <-printStats.C:
			dht.PrintStats()
		case <-stopCrawl.C:
			fmt.Println("Stopping crawl...")
			cancel()
			return nil
		case <-ch:
			fmt.Println("Stopping crawl from user interrupt...")
			cancel()
			return nil
		}
	}
}

func (dht *DHT) connManager(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			err := dht.tracker.Stop()
			if err != nil {
				fmt.Printf("Error stopping tracker: %v", err)
			}
			return
		default:
			totalDHTStreams := dht.tracker.NumActiveStreams()

			// If we have enough streams already, wait for a bit before continuing
			if totalDHTStreams >= MAX_ACTIVE_CONNS {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// If we have peers available to connect to, try to do that
			if peer := dht.tracker.PopConnectable(); peer != nil {
				go dht.tracker.StartWorker(ctx, peer)
			}
		}
	}
}

func (dht *DHT) handleIncomingConn(s network.Stream) {
	// Get info on remote peer connecting to us
	pid := s.Conn().RemotePeer()
	addrs := dht.peerstore.Addrs(pid)

	addrInfo := peer.AddrInfo{
		ID:    pid,
		Addrs: addrs,
	}

	// Add peer to tracker
	peer, err := dht.tracker.AddIncoming(addrInfo)
	if err != nil {
		fmt.Printf("Error adding incoming peer: %v\n", err)
		return
	}

	// Record incoming conn
	atomic.AddInt64(&dht.tracker.stats.numIncoming, 1)

	ctx, cancel := context.WithCancel(context.Background())

	go dht.tracker.doWrites(ctx, cancel, s, peer)
	go dht.tracker.doReads(ctx, cancel, s, peer, dht.tracker.disconnectInc)
}

func (dht *DHT) PrintStats() {
	strs := []string{}

	// Header
	strs = append(strs, fmt.Sprintf("========================="))
	strs = append(strs, fmt.Sprintf("CRAWL INFO:"))
	strs = append(strs, fmt.Sprintf("========================="))

	// Duration
	timeElapsed := dht.tracker.GetTimeElapsed()
	hours := uint64(timeElapsed.Hours())
	minutes := uint64(timeElapsed.Minutes()) % 60
	seconds := uint64(timeElapsed.Seconds()) % 60
	strs = append(strs, fmt.Sprintf("Time elapsed: %d hr | %d min | %d sec", hours, minutes, seconds)) // TODO

	// Basic info
	totalSeen := dht.tracker.GetTotalSeen()
	strs = append(strs, fmt.Sprintf("Unique peers discovered: %d", totalSeen))

	// Activity
	outboundConns, incomingConns, reads, writes := dht.tracker.GetActivity()
	strs = append(strs, fmt.Sprintf("Current # streams: %d", dht.tracker.NumActiveStreams()))
	strs = append(strs, fmt.Sprintf("Current # outbound connections: %d", outboundConns))
	strs = append(strs, fmt.Sprintf("Current # incoming connections: %d", incomingConns))
	strs = append(strs, fmt.Sprintf("Current active writes: %d", reads))
	strs = append(strs, fmt.Sprintf("Current active reads: %d", writes))

	numWrites, sentFindNode, sentPing := dht.tracker.GetNumWrites()
	strs = append(strs, fmt.Sprintf("Messages written: %d; by type:", numWrites))
	strs = append(strs, fmt.Sprintf("- FIND_NODE: %d", sentFindNode))
	strs = append(strs, fmt.Sprintf("- PING: %d", sentPing))

	numReads, readPutValue, readGetValue, readAddProvider, readGetProviders, readFindNode, readPing := dht.tracker.GetNumReads()
	strs = append(strs, fmt.Sprintf("Messages read: %d; by type:", numReads))
	strs = append(strs, fmt.Sprintf("- PUT_VALUE: %d", readPutValue))
	strs = append(strs, fmt.Sprintf("- GET_VALUE: %d", readGetValue))
	strs = append(strs, fmt.Sprintf("- ADD_PROVIDER: %d", readAddProvider))
	strs = append(strs, fmt.Sprintf("- GET_PROVIDERS: %d", readGetProviders))
	strs = append(strs, fmt.Sprintf("- FIND_NODE: %d", readFindNode))
	strs = append(strs, fmt.Sprintf("- PING: %d", readPing))

	// TODO - info about protocols, user agents, more granular activity...

	// Print stats, separated by newline
	output := strings.Join(strs, "\n")
	fmt.Println(output)
}
