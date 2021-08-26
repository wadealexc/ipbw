package types

import (
	"sync"

	"github.com/wadeAlexC/go-events/events"
)

type PeerList struct {
	*events.Emitter
	notifySize int

	// Keep a map of the IDs in this list for quick lookups
	known *IDMap

	mu    sync.Mutex
	peers []*Peer
}

func NewPeerList(notifySize int) *PeerList {
	return &PeerList{
		Emitter:    events.NewEmitter(),
		notifySize: notifySize,
		known:      NewIDMap(),
		peers:      make([]*Peer, 0),
	}
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

	toAdd := make([]*Peer, 0, len(peers))

	// Don't add duplicates
	for _, peer := range peers {
		// Record peer as known. If this peer is not in our list yet,
		// add them.
		if added := pl.known.Add(peer.ID); added {
			toAdd = append(toAdd, peer)
		}
	}

	// Update peers
	pl.mu.Lock()
	pl.peers = append(pl.peers, toAdd...)
	newLen := len(pl.peers)
	pl.mu.Unlock()

	pl.Emit("new-peers")
	// If we're under the notify threshold, emit an event
	if newLen < pl.notifySize {
		pl.Emit("under-limit")
	}
}

// Add adds the peer to the list, doing nothing
// if the peer is already in the list.
//
// Returns true if the peer was successfully added
func (pl *PeerList) Add(p *Peer) bool {
	// Don't add duplicates
	if added := pl.known.Add(p.ID); !added {
		return false
	}

	// Update peers
	pl.mu.Lock()
	pl.peers = append(pl.peers, p)
	newLen := len(pl.peers)
	pl.mu.Unlock()

	pl.Emit("new-peers")
	// If we're under the notify threshold, emit an event
	if newLen < pl.notifySize {
		pl.Emit("under-limit")
	}

	return true
}

// Remove removes the peer from the list, doing
// nothing if the peer was not in the list.
//
// Returns true if the peer was successfully removed
func (pl *PeerList) Remove(p *Peer) bool {
	pl.mu.Lock()

	removed := false

	// Iterate over peers and remove any with matching IDs
	newList := make([]*Peer, 0, len(pl.peers))
	for _, peer := range pl.peers {
		if peer.ID == p.ID {
			removed = true
			continue
		}

		newList = append(newList, peer)
	}

	// Replace peers with new list
	pl.peers = newList
	newLen := len(pl.peers)
	pl.mu.Unlock()

	// Remove peer from known
	pl.known.Remove(p.ID)

	// If we're under the notify threshold, emit an event
	if newLen < pl.notifySize {
		pl.Emit("under-limit")
	}

	return removed
}

// Removes a peer from the list and returns it.
// If the list is empty, returns false
// Otherwise, returns true
func (pl *PeerList) Pop() (*Peer, bool) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	if len(pl.peers) == 0 {
		return nil, false
	}

	// Remove first peer from list
	p := pl.peers[0]
	pl.peers = pl.peers[1:]

	// Remove peer from known
	pl.known.Remove(p.ID)

	return p, true
}
