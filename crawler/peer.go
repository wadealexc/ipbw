package crawler

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

type Peer struct {
	ID    peer.ID
	Addrs []multiaddr.Multiaddr
	Info  *PeerInfo

	stream network.Stream
}

type PeerInfo struct {
	errMu       sync.Mutex
	readErrors  []string
	writeErrors []string

	//// INFO FOR READS
	// General read stats:
	totalBytesRead    uint64
	totalMessagesRead uint64

	readMu sync.Mutex
	// Stats on the message key namespaces:
	// Example: tNR["ipns"] => number of message keys with ipns namespace
	totalNamespaceReads map[string]uint64

	// Stats for specific message types:
	totalMsgTypeReads map[MessageType]uint64
}

func NewPeer(id peer.ID, addrs []multiaddr.Multiaddr) (*Peer, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("expected nonempty addrs for peer: %v", id.Pretty())
	}

	return &Peer{
		ID:    id,
		Addrs: addrs,
		Info: &PeerInfo{
			readErrors:          make([]string, 0),
			writeErrors:         make([]string, 0),
			totalNamespaceReads: make(map[string]uint64),
			totalMsgTypeReads:   make(map[MessageType]uint64),
		},
	}, nil
}

func (p *Peer) SetStream(stream network.Stream) error {
	if p.stream != nil {
		return fmt.Errorf("already have an open stream with peer: %v", p.ID.Pretty())
	}

	p.stream = stream
	return nil
}

func (p *Peer) GetStream() network.Stream {
	return p.stream
}

func (p *Peer) LogRead(size int) {
	atomic.AddUint64(&p.Info.totalBytesRead, uint64(size))
	atomic.AddUint64(&p.Info.totalMessagesRead, uint64(1))
}

func (p *Peer) LogGetValue(namespace string, path string) {
	p.Info.readMu.Lock()
	defer p.Info.readMu.Unlock()

	p.Info.totalMsgTypeReads[GET_VALUE]++
	p.Info.totalNamespaceReads[namespace]++
}

func (p *Peer) LogPutValue(namespace string, path string, record *DHTRecord) {
	p.Info.readMu.Lock()
	defer p.Info.readMu.Unlock()

	p.Info.totalMsgTypeReads[PUT_VALUE]++
	p.Info.totalNamespaceReads[namespace]++
}

func (p *Peer) LogAddProvider() {
	p.Info.readMu.Lock()
	defer p.Info.readMu.Unlock()

	p.Info.totalMsgTypeReads[ADD_PROVIDER]++
	// p.Info.totalNamespaceReads[namespace]++
}

func (p *Peer) LogGetProviders() {
	p.Info.readMu.Lock()
	defer p.Info.readMu.Unlock()

	p.Info.totalMsgTypeReads[GET_PROVIDERS]++
	// p.Info.totalNamespaceReads[namespace]++
}

func (p *Peer) LogFindNode() {
	p.Info.readMu.Lock()
	defer p.Info.readMu.Unlock()

	p.Info.totalMsgTypeReads[FIND_NODE]++
	// p.Info.totalNamespaceReads[namespace]++
}

func (p *Peer) LogPing() {
	p.Info.readMu.Lock()
	defer p.Info.readMu.Unlock()

	p.Info.totalMsgTypeReads[PING]++
	// p.Info.totalNamespaceReads[namespace]++
}

func (p *Peer) LogReadError(format string, a ...interface{}) int {
	p.Info.errMu.Lock()
	defer p.Info.errMu.Unlock()

	str := fmt.Sprintf(format, a...)
	p.Info.readErrors = append(p.Info.readErrors, str)

	return len(p.Info.readErrors)
}

func (p *Peer) LogWriteError(format string, a ...interface{}) int {
	p.Info.errMu.Lock()
	defer p.Info.errMu.Unlock()

	str := fmt.Sprintf(format, a...)
	p.Info.writeErrors = append(p.Info.writeErrors, str)

	return len(p.Info.writeErrors)
}
