package crawler

import (
	"encoding/json"

	"github.com/libp2p/go-libp2p-core/peer"
)

// Peer contains all the info we know for a given peer
type Peer struct {
	ID         peer.ID
	UserAgent  string    // The peer's user agent
	Protocols  []string  // What protocols this peer supports
	IsReporter bool      // Whether this peer reported other peers to us
	Ips        []string  // All known IPs/ports for this peer
	Neighbors  []peer.ID // All neighbors this peer reported to us
	Timestamp  string    // The UTC timestamp when we discovered the peer
}

func (p *Peer) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Pid       string   `json:"pid"`
		UserAgent string   `json:"userAgent"`
		Protocols []string `json:"protocols"`
		Ips       []string `json:"ips"`
		Neighbors []string `json:"neighbors"`
		Timestamp string   `json:"timestamp"`
	}{
		Pid:       p.ID.Pretty(),
		UserAgent: p.UserAgent,
		Protocols: p.Protocols,
		Ips:       p.Ips,
		Neighbors: pidsToStrings(p.Neighbors),
		Timestamp: p.Timestamp,
	})
}

// Convert []peer.ID to []string
func pidsToStrings(pids []peer.ID) []string {
	res := make([]string, len(pids))
	for i, pid := range pids {
		res[i] = pid.Pretty()
	}

	return res
}
