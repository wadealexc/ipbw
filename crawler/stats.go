package crawler

import (
	"sync"
	"time"

	"github.com/wadeAlexC/ipbw/crawler/types"
)

type DHTStats struct {
	startTime time.Time

	readMu         sync.Mutex
	totalReads     uint64
	readsByMsgType map[uint64]uint64

	writeMu         sync.Mutex
	totalWrites     uint64
	writesByMsgType map[uint64]uint64

	infoMu     sync.Mutex
	protocols  map[string]uint64
	userAgents map[string]uint64
}

func NewDHTStats() *DHTStats {
	return &DHTStats{
		readsByMsgType:  make(map[uint64]uint64),
		writesByMsgType: make(map[uint64]uint64),
		protocols:       make(map[string]uint64),
		userAgents:      make(map[string]uint64),
	}
}

func (st *DHTStats) setStartTime() {
	st.startTime = time.Now()
}

func (st *DHTStats) getTimeElapsed() time.Duration {
	return time.Since(st.startTime)
}

func (st *DHTStats) logRead(p *types.Peer, mType types.MessageType, mSize int) {

}

func (st *DHTStats) logWrite(p *types.Peer, mType types.MessageType, mSize int) {

}

func (st *DHTStats) logInboundConn(p *types.Peer) {

}

func (st *DHTStats) logDCError(p *types.Peer, err error) {

}

func (st *DHTStats) logConnError(p *types.Peer, err error) {

}
