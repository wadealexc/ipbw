package crawler

import (
	"context"
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

type EventType int

const (
	// NewPeers is emitted when the crawler finds new peers
	NewPeers EventType = iota
	// DHTQueryError is emitted when we get a QueryError from the DHT
	// QueryErrors sometimes contain interesting info
	DHTQueryError
)

// Event allows modules to get status updates from the crawler
type Event struct {
	Type  EventType
	Peers []peer.ID
	Extra string
}

type queryKey struct{}

// EventChannel is used by modules to receive events
// The context should be cancelled when a module is finished using the channel
type EventChannel struct {
	mu  sync.Mutex
	ctx context.Context
	ch  chan<- *Event
}

// waitThenClose is spawned in a goroutine when the channel is registered. This
// safely cleans up the channel when the context has been canceled.
func (e *EventChannel) waitThenClose() {
	<-e.ctx.Done()
	e.mu.Lock()
	close(e.ch)
	// 1. Signals that we're done.
	// 2. Frees memory (in case we end up hanging on to this for a while).
	e.ch = nil
	e.mu.Unlock()
}

// Sends an event on the event channel, aborting if either the passed or
// the internal context expire
func (e *EventChannel) send(ctx context.Context, ev *Event) {
	e.mu.Lock()
	// Closed.
	if e.ch == nil {
		e.mu.Unlock()
		return
	}
	// in case the passed context is unrelated, wait on both.
	select {
	case e.ch <- ev:
	case <-e.ctx.Done():
	case <-ctx.Done():
	}
	e.mu.Unlock()
}

// RegisterForEvents registers an event channel with the given context.
// The returned context can be provided to the crawler to receive Events on the
// returned channel
// The passed context MUST be canceled when the caller is no longer interested
func RegisterForEvents(ctx context.Context) (context.Context, <-chan *Event) {
	ch := make(chan *Event, 16)
	ech := &EventChannel{
		ch:  ch,
		ctx: ctx,
	}
	go ech.waitThenClose()
	return context.WithValue(ctx, queryKey{}, ech), ch
}

// PublishEvent publishes an event to the event channel
// associated with the given context, if any.
func PublishEvent(ctx context.Context, ev *Event) {
	ich := ctx.Value(queryKey{})
	if ich == nil {
		return
	}

	// We *want* to panic here.
	ech := ich.(*EventChannel)
	ech.send(ctx, ev)
}

// SubscribesToEvents returns true if the context subscribes to events.
// If this function returns false, calling `PublishEvent` on the
// context will be a no-op.
func SubscribesToEvents(ctx context.Context) bool {
	return ctx.Value(queryKey{}) != nil
}
