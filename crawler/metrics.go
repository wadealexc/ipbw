package crawler

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type DHTMetrics struct {
	activeReads  int64
	activeWrites int64

	streamMu          sync.Mutex
	activeStreams     uint64
	totalSeen         uint64
	totalConnectable  uint64
	totalConnected    uint64
	totalDisconnected uint64
	totalUnreachable  uint64

	errMu     sync.Mutex
	dhtErrors []string
}

type DHTStats struct {
	activeReads  int64
	activeWrites int64

	activeStreams     uint64
	totalSeen         uint64
	totalConnectable  uint64
	totalConnected    uint64
	totalDisconnected uint64
	totalUnreachable  uint64

	dhtErrors []string
}

func NewDHTMetrics() *DHTMetrics {
	return &DHTMetrics{}
}

func (m *DHTMetrics) LogStats(totalDHTStreams int, nStat NodeStats) {
	m.streamMu.Lock()
	defer m.streamMu.Unlock()

	m.activeStreams = uint64(totalDHTStreams)
	m.totalSeen = uint64(nStat.totalSeen)
	m.totalConnectable = uint64(nStat.totalConnectable)
	m.totalConnected = uint64(nStat.totalConnected)
	m.totalDisconnected = uint64(nStat.totalDisconnected)
	m.totalUnreachable = uint64(nStat.totalUnreachable)
}

func (m *DHTMetrics) GetStats() *DHTStats {
	return nil // TODO
}

func (m *DHTMetrics) AddActiveRead() {
	atomic.AddInt64(&m.activeReads, 1)
}

func (m *DHTMetrics) RemoveActiveRead() {
	atomic.AddInt64(&m.activeReads, -1)
}

func (m *DHTMetrics) AddActiveWrite() {
	atomic.AddInt64(&m.activeWrites, 1)
}

func (m *DHTMetrics) RemoveActiveWrite() {
	atomic.AddInt64(&m.activeWrites, -1)
}

func (m *DHTMetrics) LogError(format string, a ...interface{}) {
	m.errMu.Lock()
	defer m.errMu.Unlock()

	str := fmt.Sprintf(format, a...)
	m.dhtErrors = append(m.dhtErrors, str)
}
