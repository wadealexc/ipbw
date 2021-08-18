package crawler

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

type Peer struct {
	ID        peer.ID
	Addrs     []multiaddr.Multiaddr
	Protocols []string
	Info      *PeerInfo
}

type PeerInfo struct {
	errMu            sync.Mutex
	totalReadErrors  uint64
	readErrors       map[string]uint64
	totalWriteErrors uint64
	writeErrors      map[string]uint64

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

func NewPeer(addr peer.AddrInfo) (*Peer, error) {
	if len(addr.Addrs) == 0 {
		return nil, fmt.Errorf("expected nonempty addrs for peer: %v", addr.ID.Pretty())
	}

	return &Peer{
		ID:    addr.ID,
		Addrs: addr.Addrs,
		Info: &PeerInfo{
			readErrors:          make(map[string]uint64),
			writeErrors:         make(map[string]uint64),
			totalNamespaceReads: make(map[string]uint64),
			totalMsgTypeReads:   make(map[MessageType]uint64),
		},
	}, nil
}

func (p *Peer) SetProtocols(protos []string) error {
	if len(p.Protocols) != 0 {
		return fmt.Errorf("tried to set protocols twice for peer")
	}

	p.Protocols = protos
	return nil
}

func (p *Peer) LogRead(size int) {
	atomic.AddUint64(&p.Info.totalBytesRead, uint64(size))
	atomic.AddUint64(&p.Info.totalMessagesRead, uint64(1))
}

func (p *Peer) LogGetValue() {
	p.Info.readMu.Lock()
	defer p.Info.readMu.Unlock()

	p.Info.totalMsgTypeReads[GET_VALUE]++
	// p.Info.totalNamespaceReads[namespace]++
}

func (p *Peer) LogPutValue() {
	p.Info.readMu.Lock()
	defer p.Info.readMu.Unlock()

	p.Info.totalMsgTypeReads[PUT_VALUE]++
	// p.Info.totalNamespaceReads[namespace]++
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

func (p *Peer) LogReadError(err error) {
	p.Info.errMu.Lock()
	defer p.Info.errMu.Unlock()

	p.Info.totalReadErrors++
	p.Info.readErrors[err.Error()]++
}

func (p *Peer) LogWriteError(err error) {
	p.Info.errMu.Lock()
	defer p.Info.errMu.Unlock()

	p.Info.totalWriteErrors++
	p.Info.writeErrors[err.Error()]++
}

func (p *Peer) GetErrors() string {
	p.Info.errMu.Lock()
	defer p.Info.errMu.Unlock()

	strs := []string{}

	if p.Info.totalReadErrors != 0 {
		strs = append(strs, fmt.Sprintf("Total read errors: %d", p.Info.totalReadErrors))
		for err, amt := range p.Info.readErrors {
			strs = append(strs, fmt.Sprintf("%s: %d", err, amt))
		}
	}

	if p.Info.totalWriteErrors != 0 {
		strs = append(strs, fmt.Sprintf("Total write errors: %d", p.Info.totalWriteErrors))
		for err, amt := range p.Info.writeErrors {
			strs = append(strs, fmt.Sprintf("%s: %d", err, amt))
		}
	}

	if len(strs) == 0 {
		return ""
	}

	return strings.Join(strs, "\n")
}
