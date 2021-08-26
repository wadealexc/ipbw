package types

import (
	"sync"

	peer "github.com/libp2p/go-libp2p-core/peer"
)

type IDMap struct {
	mu  sync.Mutex
	ids map[peer.ID]struct{}
}

func NewIDMap() *IDMap {
	return &IDMap{
		ids: map[peer.ID]struct{}{},
	}
}

func (m *IDMap) Contains(id peer.ID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, contains := m.ids[id]
	return contains
}

func (m *IDMap) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.ids)
}

// Add adds the ID to the map, doing nothing if it
// is already in the map.
//
// Returns true if the ID was added.
func (m *IDMap) Add(id peer.ID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, known := m.ids[id]; known {
		return false
	}

	m.ids[id] = struct{}{}
	return true
}

// Remove removes the ID from the map, doing nothing
// if it is not in the map.
//
// Returns true if the ID was removed.
func (m *IDMap) Remove(id peer.ID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, known := m.ids[id]; !known {
		return false
	}

	delete(m.ids, id)
	return true
}
