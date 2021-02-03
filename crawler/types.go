package crawler

import (
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
)

// Report aggregates results from QueryEvents
type Report struct {
	Mu    sync.RWMutex
	Peers map[peer.ID]*Peer
}

// Peer contains all the info we know for a given peer
type Peer struct {
	IsReporter bool      // Whether this peer reported other peers to us
	Ips        []string  // All known IPs/ports for this peer
	Neighbors  []peer.ID // All neighbors this peer reported to us
	Timestamp  string    // The UTC timestamp when we discovered the peer
}
