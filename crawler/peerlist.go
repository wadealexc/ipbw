package crawler

import "sync"

type PeerList struct {
	mu sync.RWMutex
}
