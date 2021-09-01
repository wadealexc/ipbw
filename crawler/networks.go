package crawler

import (
	"fmt"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	ipfsDHT "github.com/libp2p/go-libp2p-kad-dht"
)

type NetworkName string

const (
	IPFS     NetworkName = "ipfs"
	Filecoin NetworkName = "filecoin"
)

func FromString(network string) (NetworkName, error) {
	switch NetworkName(network) {
	case IPFS:
		return IPFS, nil
	case Filecoin:
		return Filecoin, nil
	default:
		return "", fmt.Errorf("%s is not a supported network. Expected %s or %s", network, IPFS, Filecoin)
	}
}

func (nn NetworkName) IsSupported() bool {
	return nn == IPFS || nn == Filecoin
}

func (nn NetworkName) GetDHTProtocol() protocol.ID {
	switch nn {
	case IPFS:
		return "/ipfs/kad/1.0.0"
	case Filecoin:
		return "/fil/kad/testnetnet/kad/1.0.0"
	}

	return ""
}

func (nn NetworkName) GetBootstrapPeers() []peer.AddrInfo {
	switch nn {
	case Filecoin:
		return getFilecoinMainnetBootstrapInfos()
	case IPFS:
		return ipfsDHT.GetDefaultBootstrapPeerAddrInfos()
	}
	return nil
}

// From filecoin-project/lotus/build/bootstrap/mainnet.pi
var FilMainnetBootstrapPeers = []string{
	"/dns4/bootstrap-0.mainnet.filops.net/tcp/1347/p2p/12D3KooWCVe8MmsEMes2FzgTpt9fXtmCY7wrq91GRiaC8PHSCCBj",
	"/dns4/bootstrap-1.mainnet.filops.net/tcp/1347/p2p/12D3KooWCwevHg1yLCvktf2nvLu7L9894mcrJR4MsBCcm4syShVc",
	"/dns4/bootstrap-2.mainnet.filops.net/tcp/1347/p2p/12D3KooWEWVwHGn2yR36gKLozmb4YjDJGerotAPGxmdWZx2nxMC4",
	"/dns4/bootstrap-3.mainnet.filops.net/tcp/1347/p2p/12D3KooWKhgq8c7NQ9iGjbyK7v7phXvG6492HQfiDaGHLHLQjk7R",
	"/dns4/bootstrap-4.mainnet.filops.net/tcp/1347/p2p/12D3KooWL6PsFNPhYftrJzGgF5U18hFoaVhfGk7xwzD8yVrHJ3Uc",
	"/dns4/bootstrap-5.mainnet.filops.net/tcp/1347/p2p/12D3KooWLFynvDQiUpXoHroV1YxKHhPJgysQGH2k3ZGwtWzR4dFH",
	"/dns4/bootstrap-6.mainnet.filops.net/tcp/1347/p2p/12D3KooWP5MwCiqdMETF9ub1P3MbCvQCcfconnYHbWg6sUJcDRQQ",
	"/dns4/bootstrap-7.mainnet.filops.net/tcp/1347/p2p/12D3KooWRs3aY1p3juFjPy8gPN95PEQChm2QKGUCAdcDCC4EBMKf",
	"/dns4/bootstrap-8.mainnet.filops.net/tcp/1347/p2p/12D3KooWScFR7385LTyR4zU1bYdzSiiAb5rnNABfVahPvVSzyTkR",
	"/dns4/lotus-bootstrap.ipfsforce.com/tcp/41778/p2p/12D3KooWGhufNmZHF3sv48aQeS13ng5XVJZ9E6qy2Ms4VzqeUsHk",
	"/dns4/bootstrap-0.starpool.in/tcp/12757/p2p/12D3KooWGHpBMeZbestVEWkfdnC9u7p6uFHXL1n7m1ZBqsEmiUzz",
	"/dns4/bootstrap-1.starpool.in/tcp/12757/p2p/12D3KooWQZrGH1PxSNZPum99M1zNvjNFM33d1AAu5DcvdHptuU7u",
	"/dns4/node.glif.io/tcp/1235/p2p/12D3KooWBF8cpp65hp2u9LK5mh19x67ftAam84z9LsfaquTDSBpt",
	"/dns4/bootstrap-0.ipfsmain.cn/tcp/34721/p2p/12D3KooWQnwEGNqcM2nAcPtRR9rAX8Hrg4k9kJLCHoTR5chJfz6d",
	"/dns4/bootstrap-1.ipfsmain.cn/tcp/34723/p2p/12D3KooWMKxMkD5DMpSWsW7dBddKxKT7L2GgbNuckz9otxvkvByP",
}

func getFilecoinMainnetBootstrapInfos() []peer.AddrInfo {
	res := make([]peer.AddrInfo, 0)

	for _, bsPeer := range FilMainnetBootstrapPeers {
		info, err := peer.AddrInfoFromString(bsPeer)
		if err != nil {
			fmt.Printf("Error converting mainnet bs peer to AddrInfo: %v", err)
			continue
		}

		res = append(res, *info)
	}

	return res
}
