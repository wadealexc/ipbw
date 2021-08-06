package crawler

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-msgio"
)

// PeerTracker keeps track of all the peers we've heard about
// as well as each peer's connection status
// This is a dumb struct that knows little of the outside world.
// It relies on the DHT to feed it accurate info.
// Please do not lie to the PeerTracker.
type PeerTracker struct {
	mu sync.Mutex

	host host.Host

	// Contains the peer IDs being used by the crawler
	self map[peer.ID]struct{}

	// All the unique peers we've seen
	allSeen map[peer.ID]struct{}

	// All the peers we have not attempted to connect to
	allConnectable map[peer.ID]*Peer

	stats *TrackerStats

	started   bool
	startTime time.Time
}

type TrackerStats struct {
	numConnected int64
	activeReads  int64
	activeWrites int64

	messagesSent uint64
	sentFindNode uint64
	sentPing     uint64

	messagesRead     uint64
	readPutValue     uint64
	readGetValue     uint64
	readAddProvider  uint64
	readGetProviders uint64
	readFindNode     uint64
	readPing         uint64

	spMu               sync.Mutex
	supportedProtocols map[string]uint64
}

type PeerStatus uint64

const (
	NOT_CONNECTED PeerStatus = iota + 1
	CONNECTED
	DISCONNECTED
	UNREACHABLE
)

type PeerType uint64

const (
	SELF PeerType = iota + 1
	BOOTSTRAP
	KNOWN
)

func NewPeerTracker(host host.Host) *PeerTracker {
	return &PeerTracker{
		host:           host,
		self:           make(map[peer.ID]struct{}),
		allSeen:        make(map[peer.ID]struct{}),
		allConnectable: make(map[peer.ID]*Peer),
		stats: &TrackerStats{
			supportedProtocols: make(map[string]uint64),
		},
	}
}

// Add our peer ID / addresses / protocols, so we don't accidentally
// re-add ourselves if we're referred to ourself by another peer
func (pt *PeerTracker) AddSelf(id peer.ID) error {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if _, exists := pt.self[id]; exists {
		return fmt.Errorf("id %s already exists", id.Pretty())
	}

	pt.self[id] = struct{}{}
	return nil
}

// Add each peer to the peer tracker, returning the number of new peers added
func (pt *PeerTracker) AddPeers(peers []peer.AddrInfo) (newCount int) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	for _, peer := range peers {
		if _, isSelf := pt.self[peer.ID]; isSelf {
			fmt.Printf("Attempted to add self to tracker!\n")
			continue // skip
		}

		if _, seen := pt.allSeen[peer.ID]; !seen {
			// Mark peer as seen
			pt.allSeen[peer.ID] = struct{}{}
			newCount++

			// Add peer as connectable
			connPeer, err := NewPeer(peer)
			if err != nil {
				fmt.Printf("Error creating peer: %v\n", err)
				continue // skip
			}

			pt.allConnectable[peer.ID] = connPeer
		}
	}

	return newCount
}

// Attempts to connect to a bootstrap peer, returning an error if unsuccessful
func (pt *PeerTracker) BootstrapFrom(bPeer peer.AddrInfo) error {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	// Record start time
	if !pt.started {
		pt.started = true
		pt.startTime = time.Now()
	}

	p, err := NewPeer(bPeer)
	if err != nil {
		return fmt.Errorf("error creating bootstrap peer: %v", err)
	}

	return pt.tryConnect(context.Background(), p)
}

func (pt *PeerTracker) PopConnectable() *Peer {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	// If we don't have any connectable peers, return nil
	if len(pt.allConnectable) == 0 {
		return nil
	}

	var pID peer.ID
	var peer *Peer

	// Map range is a decent way to get random access
	// ... but this can probably be improved TODO
	for id, p := range pt.allConnectable {
		pID = id
		peer = p
		break
	}

	// Remove peer from allConnectable and return
	delete(pt.allConnectable, pID)
	return peer
}

func (pt *PeerTracker) StartWorker(ctx context.Context, peer *Peer) {
	err := pt.tryConnect(ctx, peer)

	if err != nil {
		fmt.Printf("Error connecting to peer %s: %v\n", peer.ID.Pretty(), err)
	}
}

// Attempt to connect to a peer. Return true if we succeed
func (pt *PeerTracker) tryConnect(ctx context.Context, peer *Peer) error {
	// Add peer to libp2p host peerstore
	pt.host.Peerstore().AddAddrs(peer.ID, peer.Addrs, time.Hour)

	// Attempt to open a stream to peer. Timeout after 10 seconds
	sCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	s, err := pt.host.NewStream(sCtx, peer.ID, DHT_PROTO)
	if err != nil {
		cancel()
		return err
	}

	atomic.AddInt64(&pt.stats.numConnected, 1)

	// Start workers to read/write for peer
	ctx, cancel = context.WithCancel(ctx)

	go pt.doWrites(ctx, cancel, s, peer)
	go pt.doReads(ctx, cancel, s, peer)

	return nil
}

func (pt *PeerTracker) disconnect(ctx context.Context, cancel context.CancelFunc, s network.Stream, peer *Peer) {
	// Close stream for reads and writes
	err := s.Reset()
	if err != nil {
		fmt.Printf("Error trying to reset stream for peer %s: %v", peer.ID.Pretty(), err)
	}

	atomic.AddInt64(&pt.stats.numConnected, -1)

	peer.PrintErrors()
	// Cancel context, which should halt reads / writes
	cancel()
}

func (pt *PeerTracker) doWrites(ctx context.Context, cancel context.CancelFunc, s network.Stream, peer *Peer) {

	atomic.AddInt64(&pt.stats.activeWrites, 1)
	defer atomic.AddInt64(&pt.stats.activeWrites, -1)

	writer := msgio.NewVarintWriter(s)

	// Write FIND_NODE every 10 sec
	writeFindNode := time.NewTicker(10 * time.Second)
	// Write PING every 30 sec
	writePing := time.NewTicker(30 * time.Second)

	for {
		// First, check to see if we're done
		select {
		case <-ctx.Done():
			return
		case <-writePing.C:
			msg := NewPingMsg()

			data, err := msg.Marshal()
			if err != nil {
				panic(err) // If we fail to marshal our own message, something is quite wrong
			}

			err = writer.WriteMsg(data)
			if err != nil {
				peer.LogWriteError("Error writing PING; stopping writes: %v", err)
				return
			}

			atomic.AddUint64(&pt.stats.sentPing, 1)
		case <-writeFindNode.C:
			// Create a random key
			key := make([]byte, 16)
			rand.Read(key)

			msg := NewFindNodeMsg(key)

			data, err := msg.Marshal()
			if err != nil {
				panic(err) // If we fail to marshal our own message, something is quite wrong
			}

			err = writer.WriteMsg(data)
			if err != nil {
				peer.LogWriteError("Error writing FIND_NODE; stopping writes: %v", err)
				return
			}

			atomic.AddUint64(&pt.stats.sentFindNode, 1)
		}

		atomic.AddUint64(&pt.stats.messagesSent, 1)
	}
}

func (pt *PeerTracker) doReads(ctx context.Context, cancel context.CancelFunc, s network.Stream, peer *Peer) {

	atomic.AddInt64(&pt.stats.activeReads, 1)
	defer atomic.AddInt64(&pt.stats.activeReads, -1)

	reader := msgio.NewVarintReaderSize(s, network.MessageSizeMax)

	peerIdentified := false

	for {
		// First, check to see if we're done
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !peerIdentified {
			protos, err := pt.host.Peerstore().GetProtocols(peer.ID)
			if err == nil {
				err = peer.SetProtocols(protos)
				if err != nil {
					fmt.Printf("error setting protocols for peer; disconnecting: %v", err)
					pt.disconnect(ctx, cancel, s, peer)
					continue
				}

				pt.stats.spMu.Lock()
				for _, proto := range protos {
					pt.stats.supportedProtocols[proto]++
				}
				pt.stats.spMu.Unlock()

				peerIdentified = true
			}

			// TODO
			// agent, err := pt.host.Peerstore().Get(peer.ID, "AgentVersion")
			// if err == nil {
			// 	peer.SetAgent(agent)
			// }
		}

		// Read message from stream, logging any errors
		// Returns nil, nil if no message was read
		msgRaw, err := reader.ReadMsg()
		if err != nil {
			reader.ReleaseMsg(msgRaw)
			peer.LogReadError("ReadMsg errored with: %v", err)

			pt.disconnect(ctx, cancel, s, peer)
			return
		} else if msgRaw == nil {
			continue
		}

		// Convert raw message to DHTMessage
		dhtMsg, err := NewDHTMsg(msgRaw)
		reader.ReleaseMsg(msgRaw) // release byte buffer in reader
		if err != nil {
			errCount := peer.LogReadError("NewDHTMsg errored with: %v", err)
			if errCount > MAX_READ_ERRORS {
				pt.disconnect(ctx, cancel, s, peer)
				return
			}
			continue
		}

		// Get message key and split into namespace / path
		// If we get an error splitting the key, handle below
		// key := dhtMsg.Key
		// namespace, path, err := record.SplitKey(key)
		// if err != nil {
		// 	errCount := peer.LogReadError("SplitKey errored with: %v for key %s", err, key)
		// 	if errCount > MAX_READ_ERRORS {
		// 		pt.disconnect(ctx, cancel, s, peer)
		// 	}
		// 	continue
		// }

		// First, check to see if the peer told us about new peers / providers:
		// TODO do things with the returned values
		pt.AddPeers(dhtMsg.CloserPeers)
		// if err != nil {
		// 	errCount := peer.LogReadError("AddReferred(Closer) errored with: %v", err)
		// 	if errCount > MAX_READ_ERRORS {
		// 		pt.disconnect(ctx, cancel, s, peer)
		// 	}
		// 	continue
		// }

		pt.AddPeers(dhtMsg.ProviderPeers)
		// if err != nil {
		// 	errCount := peer.LogReadError("AddReferred(Provider) errored with: %v", err)
		// 	if errCount > MAX_READ_ERRORS {
		// 		pt.disconnect(ctx, cancel, s, peer)
		// 	}
		// 	continue
		// }

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
			// For DHT implementation see:
			// IpfsDHT.handlePutValue(nil, nil, nil)
			if dhtMsg.Key != dhtMsg.Record.Key {
				errCount := peer.LogReadError("PUT_VALUE: key mismatch. Expected %s == %s", dhtMsg.Key, dhtMsg.Record.Key)
				if errCount > MAX_READ_ERRORS {
					pt.disconnect(ctx, cancel, s, peer)
					return
				}
				continue
			}

			peer.LogPutValue()
			atomic.AddUint64(&pt.stats.readPutValue, 1)
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
			// For DHT implementation see:
			// IpfsDHT.handleGetValue(nil, nil, nil)
			peer.LogGetValue()
			atomic.AddUint64(&pt.stats.readGetValue, 1)
		case ADD_PROVIDER: // 2
			// TODO investigate
			peer.LogAddProvider()
			atomic.AddUint64(&pt.stats.readAddProvider, 1)
		case GET_PROVIDERS: // 3
			// TODO investigate
			peer.LogGetProviders()
			atomic.AddUint64(&pt.stats.readGetProviders, 1)
		case FIND_NODE: // 4
			// TODO investigate
			// Also, we may want to reply to this query :D
			peer.LogFindNode()
			atomic.AddUint64(&pt.stats.readFindNode, 1)
		case PING: // 5
			// TODO investigate
			// Also, we may want to reply to this query :D
			peer.LogPing()
			atomic.AddUint64(&pt.stats.readPing, 1)
		default:
			errCount := peer.LogReadError("invalid message type: %d", dhtMsg.Type)
			if errCount > MAX_READ_ERRORS {
				pt.disconnect(ctx, cancel, s, peer)
				return
			}
			continue
		}

		peer.LogRead(len(msgRaw))
		atomic.AddUint64(&pt.stats.messagesRead, 1)
	}
}

func (pt *PeerTracker) NumActiveStreams() int {
	activeStreams := 0

	for _, conn := range pt.host.Network().Conns() {
		for _, stream := range conn.GetStreams() {
			if stream.Protocol() == DHT_PROTO {
				activeStreams++
				break
			}
		}
	}

	return activeStreams
}

func (pt *PeerTracker) GetTimeElapsed() time.Duration {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	return time.Since(pt.startTime)
}

func (pt *PeerTracker) GetActivity() (int64, int64, int64) {
	outboundConns := atomic.LoadInt64(&pt.stats.numConnected)
	reads := atomic.LoadInt64(&pt.stats.activeReads)
	writes := atomic.LoadInt64(&pt.stats.activeWrites)

	return outboundConns, reads, writes
}

func (pt *PeerTracker) GetTotalSeen() int {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	return len(pt.allSeen)
}

func (pt *PeerTracker) GetNumMessages() (uint64, uint64) {
	numWrites := atomic.LoadUint64(&pt.stats.messagesSent)
	numReads := atomic.LoadUint64(&pt.stats.messagesRead)
	return numWrites, numReads
}
