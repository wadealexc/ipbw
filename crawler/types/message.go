package types

import (
	"crypto/rand"
	"fmt"

	"github.com/libp2p/go-libp2p-core/peer"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	pbR "github.com/libp2p/go-libp2p-record/pb"
	"github.com/multiformats/go-multiaddr"
)

// DHTMessage is what peers use to communicate
// This is a human-readable wrapper around dht.pb.Message
type DHTMessage struct {
	Type          MessageType
	ClusterLevel  int32
	Key           string
	Record        *DHTRecord
	CloserPeers   []peer.AddrInfo
	ProviderPeers []peer.AddrInfo

	rawSize int
}

type DHTRecord struct {
	Key          string
	Value        []byte
	TimeReceived string
}

type MessageType int32

const (
	PUT_VALUE MessageType = iota
	GET_VALUE
	ADD_PROVIDER
	GET_PROVIDERS
	FIND_NODE
	PING
)

// Converts a raw byte message to a DHTMessage
func NewDHTMsg(raw []byte) (*DHTMessage, error) {
	var msg pb.Message
	err := msg.Unmarshal(raw)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling message: %v", err)
	}

	closerPeers, err := toAddrInfos(msg.CloserPeers)
	if err != nil {
		return nil, fmt.Errorf("error converting CloserPeers to AddrInfo: %v", err)
	}

	providerPeers, err := toAddrInfos(msg.ProviderPeers)
	if err != nil {
		return nil, fmt.Errorf("error converting ProviderPeers to AddrInfo: %v", err)
	}

	dhtMsg := &DHTMessage{
		Type:          MessageType(msg.GetType()),
		ClusterLevel:  msg.ClusterLevelRaw,
		Key:           string(msg.GetKey()),
		CloserPeers:   closerPeers,
		ProviderPeers: providerPeers,
		rawSize:       len(raw),
	}

	if msg.Record != nil {
		dhtMsg.Record = &DHTRecord{
			Key:          string(msg.Record.Key),
			Value:        msg.Record.Value,
			TimeReceived: msg.Record.TimeReceived,
		}
	}

	return dhtMsg, nil
}

func (msg *DHTMessage) Marshal() ([]byte, error) {

	pbMsg := &pb.Message{
		Type:            pb.Message_MessageType(msg.Type),
		ClusterLevelRaw: msg.ClusterLevel,
		Key:             []byte(msg.Key),
		// TODO figure out how to send peers (unexported type in pb.Message_Peer)
		CloserPeers:   make([]pb.Message_Peer, 0),
		ProviderPeers: make([]pb.Message_Peer, 0),
	}

	if msg.Record != nil {
		pbMsg.Record = &pbR.Record{
			Key:          []byte(msg.Record.Key),
			Value:        msg.Record.Value,
			TimeReceived: msg.Record.TimeReceived,
		}
	}

	res, err := pbMsg.Marshal()
	if err != nil {
		return nil, fmt.Errorf("error marshalling message: %v", err)
	}

	return res, nil
}

func IsKnownMsgType(t int32) bool {
	return int32(PUT_VALUE) <= t && t <= int32(PING)
}

func makeFindNodeMsg() []byte {
	// Generate random key to query
	key := make([]byte, 16)
	rand.Read(key)

	msg := &DHTMessage{
		Type: FIND_NODE,
		Key:  string(key),
	}

	res, err := msg.Marshal()
	if err != nil {
		panic(err) // If we fail to marshal our own message, something is very wrong
	}
	return res
}

func makePingMsg() []byte {
	msg := &DHTMessage{
		Type: PING,
		Key:  "",
	}

	res, err := msg.Marshal()
	if err != nil {
		panic(err) // If we fail to marshal our own message, something is very wrong
	}
	return res
}

func toAddrInfos(msgPeers []pb.Message_Peer) ([]peer.AddrInfo, error) {
	res := make([]peer.AddrInfo, 0)

	for _, mPeer := range msgPeers {
		id, err := peer.IDFromBytes([]byte(mPeer.Id))
		if err != nil {
			return nil, fmt.Errorf("error converting Message_Peer.ID from bytes: %v", err)
		}

		mAddrs := make([]multiaddr.Multiaddr, 0)

		for _, addr := range mPeer.Addrs {
			mAddr, err := multiaddr.NewMultiaddrBytes(addr)
			if err != nil {
				return nil, fmt.Errorf("error converting Message_Peer.Addrs from bytes: %v", err)
			}

			mAddrs = append(mAddrs, mAddr)
		}

		res = append(res, peer.AddrInfo{
			ID:    id,
			Addrs: mAddrs,
		})
	}

	return res, nil
}
