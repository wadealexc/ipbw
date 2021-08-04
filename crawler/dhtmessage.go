package crawler

import (
	"fmt"

	"github.com/libp2p/go-libp2p-core/peer"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
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

func NewDHTMsg(msgRaw []byte) (*DHTMessage, error) {
	var msg pb.Message
	err := msg.Unmarshal(msgRaw)
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

	return &DHTMessage{
		Type:         MessageType(msg.GetType()),
		ClusterLevel: msg.ClusterLevelRaw,
		Key:          string(msg.GetKey()),
		Record: &DHTRecord{
			Key:          string(msg.Record.Key),
			Value:        msg.Record.Value,
			TimeReceived: msg.Record.TimeReceived,
		},
		CloserPeers:   closerPeers,
		ProviderPeers: providerPeers,
	}, nil
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
