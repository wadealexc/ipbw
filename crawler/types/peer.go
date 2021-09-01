package types

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-msgio"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/wadeAlexC/go-events/events"
)

// Peer represents a connection with a peer
type Peer struct {
	*events.Emitter
	ID    peer.ID
	Addrs []ma.Multiaddr

	idMu         sync.Mutex
	protocols    []string
	protoVersion string
	userAgent    string

	stream network.Stream

	known *IDMap // All the peer IDs we've heard about from this peer
}

func NewPeer(addr peer.AddrInfo) *Peer {
	return &Peer{
		Emitter:   events.NewEmitter(),
		ID:        addr.ID,
		Addrs:     addr.Addrs,
		protocols: make([]string, 0),
		known:     NewIDMap(),
	}
}

func (p *Peer) TryConnect(host host.Host, proto protocol.ID) {
	// Establish a connection with the peer, with a 10-second timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := host.Connect(ctx, peer.AddrInfo{
		ID:    p.ID,
		Addrs: p.Addrs,
	})
	if err != nil {
		cancel()
		// fmt.Printf("Connect: %v\n", err)
		p.Emit("unreachable", p, err)
		return
	}

	stream, err := host.NewStream(ctx, p.ID, proto)
	if err != nil {
		cancel()
		// fmt.Printf("NewStream: %v\n", err)
		p.Emit("unreachable", p, err)
		return
	}

	p.stream = stream
	p.Emit("connected", p)
}

func (p *Peer) SetStream(stream network.Stream) {
	p.stream = stream
}

func (p *Peer) Disconnect() {
	p.stream.Reset()

	p.Emit("disconnected")
}

func (p *Peer) HangUp() {
	p.stream.Reset()
}

func (p *Peer) Worker(ctx context.Context, pStore peerstore.Peerstore) {
	go p.writes(ctx)
	go p.reads(ctx)
	go p.identify(ctx, pStore)
}

func (p *Peer) writes(ctx context.Context) {

	writer := msgio.NewVarintWriter(p.stream)

	findNodeTicker := time.NewTicker(10 * time.Second)
	pingTicker := time.NewTicker(30 * time.Second)

	// Perform FIND_NODE / PING writes at intervals until:
	// 1. We encounter an error
	// 2. We get a signal to stop (via ctx)
	for {
		select {
		case <-ctx.Done():
			p.Disconnect()
			return
		case <-findNodeTicker.C:
			msg := makeFindNodeMsg()

			// Write FIND_NODE to peer
			err := writer.WriteMsg(msg)
			if err != nil {
				p.Emit("error", err)
				return
			}

			p.Emit("write-message", FIND_NODE, len(msg))
		case <-pingTicker.C:
			msg := makePingMsg()

			// Write PING to peer
			err := writer.WriteMsg(msg)
			if err != nil {
				p.Emit("error", err)
				return
			}

			p.Emit("write-message", PING, len(msg))
		}
	}
}

func (p *Peer) reads(ctx context.Context) {

	reader := msgio.NewVarintReaderSize(p.stream, network.MessageSizeMax)

	for {
		select {
		case <-ctx.Done():
			p.Disconnect()
			return
		default:
		}

		// TODO handle err == io.EOF

		// Read message from peer
		msgRaw, err := reader.ReadMsg()
		if err != nil {
			reader.ReleaseMsg(msgRaw)
			p.Emit("error", err)
			return
		} else if msgRaw == nil {
			continue
		}

		// Unmarshal / convert to DHTMessage
		msg, err := NewDHTMsg(msgRaw)
		reader.ReleaseMsg(msgRaw)
		if err != nil {
			p.Emit("error", err)
			return
		}

		newPeers := make([]peer.AddrInfo, 0)

		// Collect any peers this peer is reporting to us
		for _, peer := range msg.CloserPeers {
			// Add peer to this peer's reported peers. If we already
			// knew about this peer, continue
			if added := p.known.Add(peer.ID); !added {
				continue
			}

			newPeers = append(newPeers, peer)
		}

		for _, peer := range msg.ProviderPeers {
			// Add peer to this peer's reported peers. If we already
			// knew about this peer, continue
			if added := p.known.Add(peer.ID); !added {
				continue
			}

			newPeers = append(newPeers, peer)
		}

		// Finish this read:
		p.Emit("read-message", msg.Type, msg.rawSize, newPeers)
	}
}

func (p *Peer) identify(ctx context.Context, pStore peerstore.Peerstore) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		protos := p.getProtocols(pStore)
		protoVersion := p.getProtoVersion(pStore)
		agent := p.getUserAgent(pStore)

		if len(protos) != 0 && protoVersion != "" && agent != "" {
			p.Emit("identified", protos, protoVersion, agent)
			return
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// Attempt to read the peer's protocols from the libp2p peerstore
// If we fail, returns nil
// If we succeed, but no protocols are set, returns []string{"NONE"}
func (p *Peer) getProtocols(pStore peerstore.Peerstore) []string {
	p.idMu.Lock()
	defer p.idMu.Unlock()

	if len(p.protocols) == 0 {
		protos, err := pStore.GetProtocols(p.ID)
		if err != nil {
			return nil
		} else if len(protos) == 0 {
			protos = []string{"NONE"}
		}

		p.protocols = protos
	}

	return p.protocols
}

// Attempt to read the peer's protocol version from the libp2p peerstore
// If we fail, returns ""
// If we succeed, but the version is empty, return "NONE"
func (p *Peer) getProtoVersion(pStore peerstore.Peerstore) string {
	p.idMu.Lock()
	defer p.idMu.Unlock()

	if p.protoVersion == "" {
		version, err := pStore.Get(p.ID, "ProtocolVersion")
		if err != nil {
			return ""
		} else if version == nil {
			version = "NONE"
		}

		p.protoVersion = fmt.Sprintf("%s", version)
	}

	return p.protoVersion
}

// Attempt to read the peer's user agent from the libp2p peerstore
// If we fail, returns ""
// If we succeed, but the user agent is empty, return "NONE"
func (p *Peer) getUserAgent(pStore peerstore.Peerstore) string {
	p.idMu.Lock()
	defer p.idMu.Unlock()

	if p.userAgent == "" {
		agent, err := pStore.Get(p.ID, "AgentVersion")
		if err != nil {
			return ""
		} else if agent == nil {
			agent = "NONE"
		}

		p.userAgent = fmt.Sprintf("%s", agent)
	}

	return p.userAgent
}
