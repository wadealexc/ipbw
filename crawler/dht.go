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
	"github.com/libp2p/go-libp2p-core/peerstore"
	kadDHT "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-peerstore/pstoremem"
	libp2pquic "github.com/libp2p/go-libp2p-quic-transport"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-msgio"
)

type DHT struct {
	peerstore peerstore.Peerstore
	host      host.Host

	// Tracks all the peers we've heard about, connected to,
	// and failed to connect to
	peers *NodeStore

	metrics *DHTMetrics
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

	dht := &DHT{
		peerstore: peerstore,
		host:      host,
		peers:     NewNodeStore(),
		metrics:   NewDHTMetrics(), // TODO
	}

	// kad := kadDHT.NewDHTClient(nil, nil, nil)
	// kad.GetValue(nil, "", nil)
	// kad.PutValue(nil, "", nil)

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
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", PORT),      // regular tcp connections
			fmt.Sprintf("/ip6/::/tcp/%d", PORT),           // regular tcp connections
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic", PORT), // a UDP endpoint for the QUIC transport
			fmt.Sprintf("/ip6/::/udp/%d/quic", PORT),      // a UDP endpoint for the QUIC transport
		),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.UserAgent(USER_AGENT),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating libp2p node: %v", err)
	}

	return peerstore, host, nil
}

func (dht *DHT) Start(ctx context.Context) error {

	fmt.Printf("Starting DHT...\n")

	ctx, cancel := context.WithCancel(ctx)

	// Add bootstrap peers to NodeStore
	for _, addr := range kadDHT.GetDefaultBootstrapPeerAddrInfos() {
		err := dht.peers.Connectable(addr.ID, addr.Addrs)
		if err != nil {
			cancel()
			return fmt.Errorf("error adding bootstrap peer: %v", err)
		}
	}

	// Start connecting to peers in NodeStore
	go dht.connManager(ctx)

	// Create timers to stop the crawl and print crawl stats
	stopCrawl := time.NewTicker(10 * time.Minute)
	printStats := time.NewTicker(10 * time.Second)

	ch := make(chan os.Signal)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)

	go func() {
		for {
			select {
			case <-printStats.C:
				dht.PrintStats()
			case <-stopCrawl.C:
				fmt.Println("Stopping crawl...")
				cancel()
				return
			case <-ch:
				fmt.Println("Stopping crawl from user interrupt...")
				cancel()
				return
			}
		}
	}()

	return nil
}

func (dht *DHT) connManager(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			totalDHTStreams := 0

			// Figure out how many active DHT streams we have
			for _, conn := range dht.host.Network().Conns() {
				for _, stream := range conn.GetStreams() {
					if stream.Protocol() == DHT_PROTO {
						totalDHTStreams++
						break
					}
				}
			}

			dht.metrics.LogStats(totalDHTStreams, dht.peers.GetStats())

			// If we have enough streams already, wait for a bit before continuing
			if totalDHTStreams >= MAX_ACTIVE_CONNS {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Otherwise, attempt to connect to peers until we either have
			// none left to connect to, or we have exceeded MAX_ACTIVE_CONNS
			for totalDHTStreams < MAX_ACTIVE_CONNS && dht.peers.CanConnect() {
				// Pop random peer from NodeStore
				peer, err := dht.peers.PopConnectable()
				if err != nil {
					fmt.Printf("error in PopKnownPeer: %v", err)
					break
				}

				// Attempt to connect to peer
				err = dht.tryConnect(ctx, peer)
				if err != nil {
					dht.peers.Unreachable(peer)
				} else {
					dht.peers.Connected(peer)
				}
			}
		}
	}
}

func (dht *DHT) tryConnect(ctx context.Context, peer *Peer) error {
	// Add peer to libp2p peerstore with timeout
	dht.peerstore.AddAddrs(peer.ID, peer.Addrs, time.Hour)

	sCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)

	// Attempt to open a stream to peer
	s, err := dht.host.NewStream(sCtx, peer.ID, DHT_PROTO)
	if err != nil {
		cancel()
		return fmt.Errorf("error opening stream to peer: %v", err)
	}

	err = peer.SetStream(s)
	if err != nil {
		cancel()
		return fmt.Errorf("error setting peer's stream: %v", err)
	}

	// Start workers to read/write on the new peer
	ctx, cancel = context.WithCancel(ctx)
	dht.startWork(ctx, cancel, peer)

	return nil
}

func (dht *DHT) startWork(ctx context.Context, cancel context.CancelFunc, peer *Peer) {

	go dht.doWrites(ctx, cancel, peer)

	go dht.doReads(ctx, cancel, peer)
}

func (dht *DHT) doWrites(ctx context.Context, cancel context.CancelFunc, peer *Peer) {

	// Track active write
	atomic.AddInt64(&dht.metrics.activeWrites, 1)
	defer atomic.AddInt64(&dht.metrics.activeWrites, -1)

	for {
		// First, check to see if we're done
		select {
		case <-ctx.Done():
			return
		default:
		}

		// TODO
	}
}

// TODO: Move LogReadError / errCount logic to appease DRY gods
// Probably best to add dht.disconnect as a field in each peer
func (dht *DHT) doReads(ctx context.Context, cancel context.CancelFunc, peer *Peer) {

	// Track active reads
	dht.metrics.AddActiveRead()
	defer dht.metrics.RemoveActiveRead()

	reader := msgio.NewVarintReaderSize(peer.GetStream(), network.MessageSizeMax)

	for {
		// First, check to see if we're done
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Read message from stream, logging any errors
		// Returns nil, nil if no message was read
		msgRaw, err := reader.ReadMsg()
		if err != nil {
			reader.ReleaseMsg(msgRaw)

			errCount := peer.LogReadError("ReadMsg errored with: %v", err)
			if errCount > MAX_READ_ERRORS {
				dht.disconnect(ctx, cancel, peer)
			}
			continue
		} else if msgRaw == nil {
			continue
		}

		// Unmarshal raw message and convert to DHTMessage
		dhtMsg, err := NewDHTMsg(msgRaw)
		reader.ReleaseMsg(msgRaw)
		if err != nil {
			errCount := peer.LogReadError("NewDHTMsg errored with: %v", err)
			if errCount > MAX_READ_ERRORS {
				dht.disconnect(ctx, cancel, peer)
			}
			continue
		}

		// Get message key and split into namespace / path
		// If we get an error splitting the key, handle below
		key := dhtMsg.Key
		namespace, path, err := record.SplitKey(key)
		if err != nil {
			errCount := peer.LogReadError("SplitKey errored with: %v", err)
			if errCount > MAX_READ_ERRORS {
				dht.disconnect(ctx, cancel, peer)
			}
			continue
		}

		// TODO: perform more validation on reads - ex, IPNS sig validation / PK validation

		switch dhtMsg.Type {
		case PUT_VALUE: // 0
			// Peer is telling us that some key == some value, either:
			// 1. A public key == some peer ID (namespace: "/pk")
			// 2. The latest IPNS record for some peer ID (namespace: "/ipns")
			// 3. Other (??? unknown!)
			//
			// For DHT implementation see:
			// IpfsDHT.PutValue(nil, "", nil)
			// For DHT handling see:
			// IpfsDHT.handlePutValue(nil, nil, nil)
			if dhtMsg.Key != dhtMsg.Record.Key {
				errCount := peer.LogReadError("PUT_VALUE: key mismatch. Expected %s == %s", dhtMsg.Key, dhtMsg.Record.Key)
				if errCount > MAX_READ_ERRORS {
					dht.disconnect(ctx, cancel, peer)
				}
				continue
			}

			peer.LogPutValue(namespace, path, dhtMsg.Record)
		case GET_VALUE: // 1
			// Peer is requesting a value from us, either:
			// 1. A public key corresponding to some peer ID (namespace: "/pk")
			// 2. The latest IPNS record corresponding to some peer ID (namespace: "/ipns")
			// 3. Other (??? unknown!)
			//
			// We probably don't want to reply with a value,
			// but it may be interesting to log strange things in
			// the message body - like keys that aren't pk or ipns
			//
			// For DHT implementation see:
			// IpfsDHT.GetValue(nil, "", nil)
			// For DHT handling see:
			// IpfsDHT.handleGetValue(nil, nil, nil)
			peer.LogGetValue(namespace, path)
		case ADD_PROVIDER: // 2
			// TODO investigate
			peer.LogAddProvider()
		case GET_PROVIDERS: // 3
			// TODO investigate
			peer.LogGetProviders()
		case FIND_NODE: // 4
			// TODO investigate
			// Also, we may want to reply to this query :D
			peer.LogFindNode()
		case PING: // 5
			// TODO investigate
			// Also, we may want to reply to this query :D
			peer.LogPing()
		default:
			errCount := peer.LogReadError("invalid message type: %d", dhtMsg.Type)
			if errCount > MAX_READ_ERRORS {
				dht.disconnect(ctx, cancel, peer)
			}
			continue
		}

		peer.LogRead(len(msgRaw))
		// TODO grab CloserPeers and ProviderPeers here
	}
}

// TODO improve ?
func (dht *DHT) disconnect(ctx context.Context, cancel context.CancelFunc, peer *Peer) {
	// Close stream for reads and writes
	err := peer.GetStream().Reset()
	if err != nil {
		dht.metrics.LogError("Stream.Reset on peer %s errored with: %v", peer.ID.Pretty(), err)
	}

	// Mark peer as disconnected
	dht.peers.Disconnected(peer) // TODO

	// Cancel context, which should halt doReads / doWrites
	cancel()
}

func (dht *DHT) handleIncomingConn(s network.Stream) {
	// TODO
}

func (dht *DHT) PrintStats() {
	strs := []string{}

	// Header
	strs = append(strs, fmt.Sprintf("========================="))
	strs = append(strs, fmt.Sprintf("CRAWL INFO:"))
	strs = append(strs, fmt.Sprintf("========================="))

	stats := dht.metrics.GetStats() // TODO impl

	// Duration
	strs = append(strs, fmt.Sprintf("Time elapsed: %dhr%dmin%dsec", 1, 1, 1)) // TODO

	// Network
	strs = append(strs, fmt.Sprintf("Total peer IDs seen: %d", stats.totalSeen))
	// TODO - info about protocols

	// Worker activity
	strs = append(strs, fmt.Sprintf("%d peers currently connected", stats.totalConnected))
	strs = append(strs, fmt.Sprintf("%d streams using DHT protocol", stats.activeStreams))
	strs = append(strs, fmt.Sprintf("%d active writes with peers", stats.activeWrites))
	strs = append(strs, fmt.Sprintf("%d active reads with peers", stats.activeReads))
	// TODO - how much data has been read/written; how many messages sent/received?

	strs = append(strs, fmt.Sprintf("%d peers waiting to be connected to", stats.totalConnectable))
	strs = append(strs, fmt.Sprintf("%d peers disconnected", stats.totalDisconnected))
	strs = append(strs, fmt.Sprintf("%d peers found unreachable", stats.totalDisconnected))

	// Errors
	// TODO - DHT errors, any peer errors?

	// Print stats, separated by newline
	output := strings.Join(strs, "\n")
	fmt.Println(output)
}
