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

func NewPeer(addr peer.AddrInfo) (*Peer, error) {
	if len(addr.Addrs) == 0 {
		return nil, fmt.Errorf("expected nonempty addrs for peer: %v", addr.ID.Pretty())
	}

	return &Peer{
		ID:    addr.ID,
		Addrs: addr.Addrs,
		Info: &PeerInfo{
			readErrors:          make([]string, 0),
			writeErrors:         make([]string, 0),
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

func (p *Peer) GetErrors() string {
	p.Info.errMu.Lock()
	defer p.Info.errMu.Unlock()

	strs := []string{}

	if len(p.Info.readErrors) != 0 {
		strs = append(strs, fmt.Sprintf("Read errors:"))
		for _, err := range p.Info.readErrors {
			strs = append(strs, err)
		}
	}

	if len(p.Info.writeErrors) != 0 {
		strs = append(strs, fmt.Sprintf("Write errors:"))
		for _, err := range p.Info.writeErrors {
			strs = append(strs, err)
		}
	}

	if len(strs) == 0 {
		return ""
	}

	return strings.Join(strs, "\n")
}
