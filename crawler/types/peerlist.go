package types

import (
	"sync"

	"github.com/wadeAlexC/go-events/events"
)

type PeerList struct {
	*events.Emitter
	notifySize int

	mu    sync.Mutex
	peers []*Peer
}

func NewPeerList() *PeerList {
	return &PeerList{
		Emitter: events.NewEmitter(),
		peers:   make([]*Peer, 0),
	}
}

func (pl *PeerList) SetNotifySize(size int) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	pl.notifySize = size
}

func (pl *PeerList) Count() int {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	return len(pl.peers)
}

func (pl *PeerList) AddAll(peers []*Peer) {
	if len(peers) == 0 {
		return
	}

	pl.mu.Lock()
	pl.peers = append(pl.peers, peers...)
	pl.mu.Unlock()

	pl.Emit("new-peers")
	// If we're under the notify threshold, emit an event
	if len(pl.peers) < pl.notifySize {
		pl.Emit("under-limit")
	}
}

func (pl *PeerList) Add(p *Peer) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	pl.peers = append(pl.peers, p)

	pl.Emit("new-peers")
	// If we're under the notify threshold, emit an event
	if len(pl.peers) < pl.notifySize {
		pl.Emit("under-limit")
	}
}

func (pl *PeerList) Remove(p *Peer) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	// Iterate over peers and remove any with matching IDs
	newList := make([]*Peer, 0, len(pl.peers))
	for _, peer := range pl.peers {
		if peer.ID == p.ID {
			continue
		}

		newList = append(newList, peer)
	}

	// Replace peers with new list
	pl.peers = newList

	// If we're under the notify threshold, emit an event
	if len(pl.peers) < pl.notifySize {
		pl.Emit("under-limit")
	}
}

func (pl *PeerList) Pop() (*Peer, bool) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	if len(pl.peers) == 0 {
		return nil, true
	}

	// Remove first peer from list
	p := pl.peers[0]
	pl.peers = pl.peers[1:]

	return p, false
}
