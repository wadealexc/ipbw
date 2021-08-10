package crawler

import (
	"fmt"

	peer "github.com/libp2p/go-libp2p-core/peer"
)

// From filecoin-project/lotus/build/bootstrap/mainnet.pi
var MainnetBootstrapPeers = []string{
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

// This may not be correct :/
var MainnetProtoName = "/fil/kad/mainnet"

// From filecoin-project/lotus/build/bootstrap/nerpanet.pi
var NerpanetBootstrapPeers = []string{
	"/dns4/bootstrap-2.nerpa.interplanetary.dev/tcp/1347/p2p/12D3KooWQcL6ReWmR6ASWx4iT7EiAmxKDQpvgq1MKNTQZp5NPnWW",
	"/dns4/bootstrap-0.nerpa.interplanetary.dev/tcp/1347/p2p/12D3KooWGyJCwCm7EfupM15CFPXM4c7zRVHwwwjcuy9umaGeztMX",
	"/dns4/bootstrap-3.nerpa.interplanetary.dev/tcp/1347/p2p/12D3KooWNK9RmfksKXSCQj7ZwAM7L6roqbN4kwJteihq7yPvSgPs",
	"/dns4/bootstrap-1.nerpa.interplanetary.dev/tcp/1347/p2p/12D3KooWCWSaH6iUyXYspYxELjDfzToBsyVGVz3QvC7ysXv7wESo",
}

// This may not be correct :/
var NerpanetProtoName = "/fil/kad/nerpanet"

func GetFilecoinMainnetBootstrapInfos() []peer.AddrInfo {
	res := make([]peer.AddrInfo, 0)

	for _, bsPeer := range MainnetBootstrapPeers {
		info, err := peer.AddrInfoFromString(bsPeer)
		if err != nil {
			fmt.Printf("Error converting mainnet bs peer to AddrInfo: %v", err)
			continue
		}

		res = append(res, *info)
	}

	return res
}

func GetFilecoinNerpanetBootstrapInfos() []peer.AddrInfo {
	res := make([]peer.AddrInfo, 0)

	for _, bsPeer := range NerpanetBootstrapPeers {
		info, err := peer.AddrInfoFromString(bsPeer)
		if err != nil {
			fmt.Printf("Error converting mainnet bs peer to AddrInfo: %v", err)
			continue
		}

		res = append(res, *info)
	}

	return res
}
