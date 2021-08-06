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
	"github.com/multiformats/go-multiaddr"
)

// PeerTracker keeps track of all the peers we've heard about
// as well as each peer's connection status
// This is a dumb struct that knows little of the outside world.
// It relies on the DHT to feed it accurate info.
// Please do not lie to the PeerTracker.
type PeerTracker struct {
	mu sync.Mutex

	host host.Host

	// Separate peer IDs into SELF, BOOTSTRAP, and KNOWN
	peers map[peer.ID]PeerType

	// Peers that belong to our crawler
	self []*Peer
	// Peer IDs we bootstrapped from
	bootstrap map[PeerStatus][]*Peer
	// Peers we have heard about during our crawl
	known map[PeerStatus][]*Peer

	stats *TrackerStats

	started   bool
	startTime time.Time
}

type TrackerStats struct {
	activeStreams int64
	activeReads   int64
	activeWrites  int64

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
		host:      host,
		peers:     make(map[peer.ID]PeerType),
		self:      make([]*Peer, 0),
		bootstrap: make(map[PeerStatus][]*Peer),
		known:     make(map[PeerStatus][]*Peer),
		stats: &TrackerStats{
			supportedProtocols: make(map[string]uint64),
		},
	}
}

// Add our peer ID / addresses / protocols, so we don't accidentally
// re-add ourselves if we're referred to ourself by another peer
func (pt *PeerTracker) AddSelf(id peer.ID, addrs []multiaddr.Multiaddr, protocols []string) error {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if len(protocols) == 0 {
		return fmt.Errorf("expected self to have protocols")
	}

	p, err := NewPeer(id, addrs, protocols)
	if err != nil {
		return fmt.Errorf("error creating new peer for self: %v", err)
	}

	// Make sure we don't add twice
	if _, exists := pt.peers[id]; exists {
		return fmt.Errorf("tried to add self twice")
	}

	pt.peers[id] = SELF
	pt.self = append(pt.self, p)
	return nil
}

// Add bootstrap peers to tracker. Bootstrap peers don't have a referring peer
func (pt *PeerTracker) AddBootstrap(id peer.ID, addrs []multiaddr.Multiaddr) error {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	p, err := NewPeer(id, addrs, nil)
	if err != nil {
		return fmt.Errorf("error creating new bootstrap peer: %v", err)
	}

	// Make sure we don't add twice
	if _, exists := pt.peers[id]; exists {
		return fmt.Errorf("tried to add bootstrap peer twice")
	}

	pt.peers[id] = BOOTSTRAP
	pt.bootstrap[NOT_CONNECTED] = append(pt.bootstrap[NOT_CONNECTED], p)

	fmt.Printf("Bootstrap peer %s has addrs:%v\n", p.ID.Pretty(), p.Addrs)

	return nil
}

// Add a peer to the tracker and mark the peer that referred us
// TODO credit referPeer somehow
func (pt *PeerTracker) AddReferred(referPeer peer.ID, newPeers []peer.AddrInfo) (selfCount int, newUnique int, err error) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	for _, newPeer := range newPeers {
		if pt.isSelf(newPeer) {
			selfCount++
		} else if !pt.isKnown(newPeer) {
			newUnique++

			// New unknown peer: add to tracker
			p, err := NewPeer(newPeer.ID, newPeer.Addrs, nil)
			if err != nil {
				return 0, 0, fmt.Errorf("error adding referred peer to tracker: %v", err)
			}

			pt.peers[newPeer.ID] = KNOWN
			pt.known[NOT_CONNECTED] = append(pt.known[NOT_CONNECTED], p)
		}
	}

	return selfCount, newUnique, nil
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

	return time.Now().Sub(pt.startTime)
}

func (pt *PeerTracker) GetActivity() (int64, int64, int64) {
	streams := int64(pt.NumActiveStreams())
	reads := atomic.LoadInt64(&pt.stats.activeReads)
	writes := atomic.LoadInt64(&pt.stats.activeWrites)

	return streams, reads, writes
}

func (pt *PeerTracker) GetTotalSeen() int {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	return len(pt.peers) - len(pt.self)
}

func (pt *PeerTracker) GetReadsWrites() (uint64, uint64) {
	numWrites := atomic.LoadUint64(&pt.stats.messagesSent)
	numReads := atomic.LoadUint64(&pt.stats.messagesRead)
	return numWrites, numReads
}

// We have work if we have NOT_CONNECTED peers
func (pt *PeerTracker) HasWork() bool {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	return len(pt.bootstrap[NOT_CONNECTED]) != 0 || len(pt.known[NOT_CONNECTED]) != 0
}

// Assigns a worker to a NOT_CONNECTED peer
// Returns an error if we can't connect to any peers
func (pt *PeerTracker) StartWorker(ctx context.Context) error {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if !pt.started {
		fmt.Printf("Starting first worker!\n")
		pt.startTime = time.Now()
		pt.started = true
	}

	// Yikes, this is verbose! TODO: fix that.

	// Get a NOT_CONNECTED peer, prioritizing bootstrap:
	for idx, bPeer := range pt.bootstrap[NOT_CONNECTED] {
		err := pt.tryConnect(ctx, bPeer)

		// Since we've tried to connect to this peer, remove them from NOT_CONNECTED
		// We're going to place them in CONNECTED or UNREACHABLE
		pt.bootstrap[NOT_CONNECTED][idx] = pt.bootstrap[NOT_CONNECTED][len(pt.bootstrap[NOT_CONNECTED])-1]
		pt.bootstrap[NOT_CONNECTED] = pt.bootstrap[NOT_CONNECTED][:len(pt.bootstrap[NOT_CONNECTED])-1]

		if err == nil {
			pt.bootstrap[CONNECTED] = append(pt.bootstrap[CONNECTED], bPeer)
			fmt.Printf("Connected to bootstrap peer %s with addrs %v\n", bPeer.ID.Pretty(), bPeer.Addrs)
			return nil // we connected; we're done
		} else {
			pt.bootstrap[UNREACHABLE] = append(pt.bootstrap[UNREACHABLE], bPeer)
			return fmt.Errorf("error connecting to bootstrap peer: %v", err)
		}
	}

	// Get a NOT_CONNECTED peer from non-bootstrap peers:
	for idx, ncPeer := range pt.known[NOT_CONNECTED] {
		err := pt.tryConnect(ctx, ncPeer)

		// Since we've tried to connect to this peer, remove them from NOT_CONNECTED
		// We're going to place them in CONNECTED or UNREACHABLE
		pt.known[NOT_CONNECTED][idx] = pt.known[NOT_CONNECTED][len(pt.known[NOT_CONNECTED])-1]
		pt.known[NOT_CONNECTED] = pt.known[NOT_CONNECTED][:len(pt.known[NOT_CONNECTED])-1]

		if err == nil {
			pt.known[CONNECTED] = append(pt.known[CONNECTED], ncPeer)
			return nil // we connected; we're done
		} else {
			pt.known[UNREACHABLE] = append(pt.known[UNREACHABLE], ncPeer)
			return fmt.Errorf("error connecting to known peer: %v", err)
		}
	}

	return fmt.Errorf("unable to connect to any peers")
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

	peer.PrintErrors()

	// Move peer from CONNECTED to DISCONNECTED
	err = pt.markDisconnected(peer)
	if err != nil {
		fmt.Printf("error marking peer as disconnected: %v", err)
	}

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
				peer.LogWriteError("Error writing PING; WriteMsg errored with: %v", err)
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
				peer.LogWriteError("Error writing FIND_NODE; WriteMsg errored with: %v", err)
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
		_, _, err = pt.AddReferred(peer.ID, dhtMsg.CloserPeers)
		if err != nil {
			errCount := peer.LogReadError("AddReferred(Closer) errored with: %v", err)
			if errCount > MAX_READ_ERRORS {
				pt.disconnect(ctx, cancel, s, peer)
			}
			continue
		}

		_, _, err = pt.AddReferred(peer.ID, dhtMsg.ProviderPeers)
		if err != nil {
			errCount := peer.LogReadError("AddReferred(Provider) errored with: %v", err)
			if errCount > MAX_READ_ERRORS {
				pt.disconnect(ctx, cancel, s, peer)
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
			// For DHT implementation see:
			// IpfsDHT.handlePutValue(nil, nil, nil)
			if dhtMsg.Key != dhtMsg.Record.Key {
				errCount := peer.LogReadError("PUT_VALUE: key mismatch. Expected %s == %s", dhtMsg.Key, dhtMsg.Record.Key)
				if errCount > MAX_READ_ERRORS {
					pt.disconnect(ctx, cancel, s, peer)
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
			}
			continue
		}

		peer.LogRead(len(msgRaw))
		atomic.AddUint64(&pt.stats.messagesRead, 1)
	}
}

func (pt *PeerTracker) isSelf(peer peer.AddrInfo) bool {
	for _, self := range pt.self {
		if self.ID == peer.ID {
			return true
		}
	}

	return false
}

func (pt *PeerTracker) isKnown(peer peer.AddrInfo) bool {
	_, exists := pt.peers[peer.ID]
	return exists
}

// TODO: messy af
func (pt *PeerTracker) markDisconnected(peer *Peer) error {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pType := pt.peers[peer.ID]
	if pType == BOOTSTRAP {
		// Check in CONNECTED:
		for idx, bPeer := range pt.bootstrap[CONNECTED] {
			// Found peer - move to DISCONNECTED
			if bPeer.ID == peer.ID {
				// Remove from CONNECTED
				pt.bootstrap[CONNECTED][idx] = pt.bootstrap[CONNECTED][len(pt.bootstrap[CONNECTED])-1]
				pt.bootstrap[CONNECTED] = pt.bootstrap[CONNECTED][:len(pt.bootstrap[CONNECTED])-1]

				// Append to DISCONNECTED
				pt.bootstrap[DISCONNECTED] = append(pt.bootstrap[DISCONNECTED], peer)
				return nil
			}
		}
	} else if pType == KNOWN {
		// Check in CONNECTED:
		for idx, bPeer := range pt.known[CONNECTED] {
			// Found peer - move to DISCONNECTED
			if bPeer.ID == peer.ID {
				// Remove from CONNECTED
				pt.known[CONNECTED][idx] = pt.known[CONNECTED][len(pt.known[CONNECTED])-1]
				pt.known[CONNECTED] = pt.known[CONNECTED][:len(pt.known[CONNECTED])-1]

				// Append to DISCONNECTED
				pt.known[DISCONNECTED] = append(pt.known[DISCONNECTED], peer)
				return nil
			}
		}
	} else {
		return fmt.Errorf("expected disconnected peer to be BOOTSTRAP or KNOWN, got: %d", pType)
	}

	return fmt.Errorf("unable to find %v peer %s in CONNECTED list", pType, peer.ID.Pretty())
}
