package types

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-msgio"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/wadeAlexC/go-events/events"
)

const DHT_PROTO = "/ipfs/kad/1.0.0"

// Peer represents a connection with a peer
type Peer struct {
	*events.Emitter
	ID    peer.ID
	Addrs []ma.Multiaddr

	stream network.Stream

	known *IDMap // All the peer IDs we've heard about from this peer
}

func NewPeer(addr peer.AddrInfo) *Peer {
	return &Peer{
		Emitter: events.NewEmitter(),
		ID:      addr.ID,
		Addrs:   addr.Addrs,
		known:   NewIDMap(),
	}
}

func (p *Peer) TryConnect(host host.Host) {
	// Add addresses to host peerstore
	host.Peerstore().AddAddrs(p.ID, p.Addrs, time.Hour)

	// Attempt to open a stream to peer. Timeout after 10 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	stream, err := host.NewStream(ctx, p.ID, DHT_PROTO)
	if err != nil {
		cancel()
		p.Emit("unreachable", err)
		return
	}

	p.stream = stream
	p.Emit("connected")
}

func (p *Peer) SetStream(stream network.Stream) {
	p.stream = stream
}

func (p *Peer) Disconnect() {
	p.stream.Reset()

	p.Emit("disconnected")
}

func (p *Peer) Worker(ctx context.Context) {
	go p.writes(ctx)
	go p.reads(ctx)
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
			p.stream.Reset()
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
			p.stream.Reset()
			return
		default:
		}

		// TODO add identification handling here
		// (protos / user agents / etc)

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
