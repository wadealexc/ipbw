package crawler

import (
	"fmt"
	"math/rand"
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

// NodeStore keeps track of all the peers we've heard about
// as well as each peer's connection status
// This is a dumb struct that knows little of the outside world.
// It relies on the DHT to feed it accurate info.
// Please do not lie to the NodeStore.
type NodeStore struct {
	mu           sync.Mutex
	seen         map[peer.ID]struct{}
	connectable  []*Peer // peers we have not attempted to connect to
	connected    []*Peer // peers we are currently connected to
	disconnected []*Peer // peers we were connected to, but no longer
	unreachable  []*Peer // peers we were unable to connect to
}

type NodeStats struct {
	totalSeen         int
	totalConnectable  int
	totalConnected    int
	totalDisconnected int
	totalUnreachable  int
}

func NewNodeStore() *NodeStore {
	return &NodeStore{
		seen:         map[peer.ID]struct{}{},
		connectable:  make([]*Peer, 0),
		connected:    make([]*Peer, 0),
		disconnected: make([]*Peer, 0),
		unreachable:  make([]*Peer, 0),
	}
}

func (ns *NodeStore) GetStats() NodeStats {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	return NodeStats{
		totalSeen:         len(ns.seen),
		totalConnectable:  len(ns.connectable),
		totalConnected:    len(ns.connected),
		totalDisconnected: len(ns.disconnected),
		totalUnreachable:  len(ns.unreachable),
	}
}

func (ns *NodeStore) CanConnect() bool {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	return len(ns.connectable) == 0
}

func (ns *NodeStore) Connectable(id peer.ID, addrs []multiaddr.Multiaddr) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	// Add peer to NodeStore if we have not seen them yet
	if _, exists := ns.seen[id]; !exists {
		peer, err := NewPeer(id, addrs)
		if err != nil {
			return fmt.Errorf("error creating new peer: %v", err)
		}
		// Mark peer seen, and add them to connectable
		ns.seen[id] = struct{}{}
		ns.connectable = append(ns.connectable, peer)
	}

	return nil
}

func (ns *NodeStore) PopConnectable() (*Peer, error) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	if len(ns.connectable) == 0 {
		return nil, fmt.Errorf("no connectable peers in NodeStore")
	}

	// Get random connectable peer
	idx := rand.Intn(len(ns.connectable))
	knownPeer := ns.connectable[idx]

	// Replace in connectable with last element, then shrink slice
	ns.connectable[idx] = ns.connectable[len(ns.connectable)-1]
	ns.connectable = ns.connectable[:len(ns.connectable)-1]

	return knownPeer, nil
}

func (ns *NodeStore) Connected(peer *Peer) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	ns.connected = append(ns.connected, peer)
}

func (ns *NodeStore) Unreachable(peer *Peer) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	ns.unreachable = append(ns.unreachable, peer)
}

// TODO - remove from connected?
func (ns *NodeStore) Disconnected(peer *Peer) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	ns.disconnected = append(ns.disconnected, peer)
}
