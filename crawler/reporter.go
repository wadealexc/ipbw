package crawler

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	ma "github.com/multiformats/go-multiaddr"
	"go.uber.org/fx"
)

// Reporter aggregates query results from Client.Events into Report
// It also prints metrics to console after some ReportInterval
type Reporter struct {
	*Client

	*Report
	ReportInterval uint
}

// Report aggregates results from QueryEvents
type Report struct {
	mu    sync.RWMutex
	peers map[peer.ID]*Peer
}

// Peer contains all the info we know for a given peer
type Peer struct {
	isReporter bool      // Whether this peer reported other peers to us
	ips        []string  // All known IPs/ports for this peer
	neighbors  []peer.ID // All neighbors this peer reported to us
	timestamp  string    // The UTC timestamp when we discovered the peer
}

type reporterParams struct {
	fx.In
	Client *Client
}

// NewReporter creates our reporter and defines start/stop hooks
func NewReporter(params reporterParams, lc fx.Lifecycle) (*Reporter, error) {

	reporter := &Reporter{
		Client: params.Client,
		Report: &Report{
			peers: make(map[peer.ID]*Peer),
		},
	}

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			return reporter.start()
		},
		OnStop: func(context.Context) error {
			return reporter.stop()
		},
	})

	return reporter, nil
}

// SetInterval describes how often a report is printed to console
func (r *Reporter) SetInterval(reportInterval uint) error {
	if reportInterval == 0 {
		return fmt.Errorf("Expected nonzero report interval")
	}
	r.ReportInterval = reportInterval
	return nil
}

func (r *Reporter) start() error {
	go r.aggregator()
	go r.printer()
	return nil
}

func (r *Reporter) stop() error {
	return nil
}

func (r *Reporter) aggregator() {
	// Get our PeerID, so we can filter responses that come from our own node
	self := r.Client.DHT.PeerID()

	for report := range r.Client.Events {
		// We only care about PeerResponse events, as these
		// are peers informing us of peers they know.
		if report.Type != routing.PeerResponse {
			continue
		}

		// Get reporting peer, the IDs/Addrs they reported, and a timestamp
		reporter := report.ID
		response := r.filterSelf(self, report)
		timestamp := time.Now().UTC().String()

		// No responses? Skip!
		if len(response) == 0 {
			continue
		}

		r.Report.mu.RLock() // Lock for reads

		// Check if this reporter exists in our report yet. If not, create an entry:
		if _, exists := r.Report.peers[reporter]; !exists {
			r.Report.peers[reporter] = &Peer{
				isReporter: true,
				ips:        make([]string, 0), // TODO we can probably query our PeerStore here
				neighbors:  make([]peer.ID, 0),
				timestamp:  timestamp,
			}
		} else if !r.Report.peers[reporter].isReporter {
			// They exist, but have not been a reporter yet
			r.Report.peers[reporter].isReporter = true
		}

		// We need to update our reporter's neighbors with the IDs they just gave us.
		// First, figure out what neighbors we already know about:
		seenID := map[peer.ID]struct{}{}
		neighbors := make([]peer.ID, 0)
		seenID[reporter] = struct{}{} // No, you aren't your own neighbor

		// Range over already-known neighbors
		for _, neighbor := range r.Report.peers[reporter].neighbors {
			// Record as neighbor and filter duplicates
			if _, seen := seenID[neighbor]; !seen {
				seenID[neighbor] = struct{}{}
				neighbors = append(neighbors, neighbor)
			}
		}

		// Next, for each newly-reported neighbor:
		// 1. Record them as one of our reporter's neighbors
		// 2. Add them to c.report.peers if they don't exist yet
		// 3. Update their report entry with the new IPs we have for them
		for _, peerInfo := range response {
			// 1. Record as neighbor and filter duplicates
			if _, seen := seenID[peerInfo.ID]; !seen {
				seenID[peerInfo.ID] = struct{}{}
				neighbors = append(neighbors, peerInfo.ID)
			}

			// 2. If we haven't seen this peer yet, create an entry for them
			if _, exists := r.Report.peers[peerInfo.ID]; !exists {
				r.Report.peers[peerInfo.ID] = &Peer{
					ips:       make([]string, 0),
					neighbors: []peer.ID{reporter}, // Our reporter is the first neighbor
					timestamp: timestamp,
				}
			}

			// 3. Update report entry with the new IPs we have for them
			// First, figure out what IPs we already know about:
			seenIP := map[string]struct{}{}
			knownIPs := make([]string, 0)

			// Range over already-known IPs
			for _, ip := range r.Report.peers[peerInfo.ID].ips {
				// Filter duplicates
				if _, seen := seenIP[ip]; !seen {
					seenIP[ip] = struct{}{}
					knownIPs = append(knownIPs, ip)
				}
			}

			// Range over newly-reported IPs
			for _, ip := range r.filterIPs(peerInfo.Addrs) {
				// Filter duplicates
				if _, seen := seenIP[ip]; !seen {
					seenIP[ip] = struct{}{}
					knownIPs = append(knownIPs, ip)
				}
			}

			// Finally, add this peer's known IPs to the report:
			if len(knownIPs) != 0 {
				r.Report.peers[peerInfo.ID].ips = knownIPs
			}
		}

		// Finally finally, record our reporter's neighbors
		if len(neighbors) != 0 {
			r.Report.peers[reporter].neighbors = neighbors
		}

		// Unlock map for reads
		r.Report.mu.RUnlock()
	}
}

func (r *Reporter) printer() {
	startTime := time.Now()
	reportInterval := time.Duration(r.ReportInterval) * time.Minute
	for {
		select {
		case <-time.After(reportInterval):
			timeElapsed := uint(time.Now().Sub(startTime).Minutes())

			// Tally results
			results := r.getResults()

			outputArr := make([]string, 10)
			// VERY important to get this right or the whole world will burn
			if timeElapsed == 1 {
				outputArr = append(outputArr, fmt.Sprintf("--- Time elapsed: %d minute ---\n", timeElapsed))
			} else {
				outputArr = append(outputArr, fmt.Sprintf("--- Time elapsed: %d minutes ---\n", timeElapsed))
			}

			outputArr = append(outputArr, fmt.Sprintf(">> %d peers reporting:\n", results.numReporters))
			outputArr = append(outputArr, fmt.Sprintf("- %d unique peer IDs -\n", results.uniquePeers))
			outputArr = append(outputArr, fmt.Sprintf("- %d peers with a valid IP+port -\n", results.peersWithIPs))
			outputArr = append(outputArr, fmt.Sprintf("- %d unique IP+port combinations -\n", results.uniqueTargets))

			output := strings.Join(outputArr, "")
			fmt.Printf(output)
		}
	}
}

// Results holds printable metrics about our crawl
type Results struct {
	numReporters  uint // Number of peers that responded to our queries
	uniquePeers   uint // Number of peers that we have an ID for
	peersWithIPs  uint // Number of peers we have an IP address for
	uniqueTargets uint // Number of unique IP+port combinations we have
}

// Iterates over our report and retrieves printable metrics
func (r *Reporter) getResults() *Results {

	r.Report.mu.Lock()
	defer r.Report.mu.Unlock()

	results := &Results{
		uniquePeers: uint(len(r.Report.peers)),
	}

	// Track which IPs we've seen for unique target tally
	seenIP := map[string]struct{}{}

	for _, peer := range r.Report.peers {
		if peer.isReporter {
			results.numReporters++
		}

		if len(peer.ips) != 0 {
			results.peersWithIPs++
		}

		for _, ip := range peer.ips {
			if _, seen := seenIP[ip]; !seen {
				results.uniqueTargets++
				seenIP[ip] = struct{}{}
			}
		}
	}

	return results
}

// Filter DHT query responses that contain our own peer ID
func (r *Reporter) filterSelf(self peer.ID, report *routing.QueryEvent) []*peer.AddrInfo {
	var res []*peer.AddrInfo = make([]*peer.AddrInfo, 0)

	// We don't want reports from our own node
	if report.ID == self {
		fmt.Printf("Unexpected self-report!\n")
		return res
	}

	// We also don't want reports that contain our own node
	responsesRaw := report.Responses
	for _, info := range responsesRaw {
		if info.ID.String() != self.String() {
			res = append(res, info)
		}
	}
	return res
}

// Iterates over a collection of multiaddrs and attempts to find ip+port combos
func (r *Reporter) filterIPs(addrs []ma.Multiaddr) []string {
	var results []string = make([]string, 0)
	for _, addr := range addrs {
		fields := strings.Split(addr.String(), "/")

		var (
			res       string
			portFound bool
			ipFound   bool
		)
		// Iterate over the split multiaddr. First, look for an IP (IPv4, IPv6, DNS4, DNS6)
		// Then, find a port (UDP, TCP)
		for i, field := range fields {
			if field == "ip4" || field == "ip6" || field == "dns" || field == "dns4" || field == "dns6" || field == "dnsaddr" { // found ip
				if res != "" {
					fmt.Printf("Possible malformed multiaddr %s; skipping\n", addr.String())
					break
				} else if i+1 >= len(fields) {
					fmt.Printf("Unexpected EOField in %s; skipping\n", addr.String())
					break
				} else if fields[i+1] == "127.0.0.1" {
					// Skip localhost
					break
				}

				ipRaw := fields[i+1]

				// If we parsed a DNS address, we're done
				if field == "dns" || field == "dns4" || field == "dns6" || field == "dnsaddr" {
					res = ipRaw
					ipFound = true
					break
				}

				// For IPv4 / IPv6 addresses, filter out private / local / loopback IPs
				ip := net.ParseIP(ipRaw)
				switch {
				case ip == nil:
					fmt.Printf("Invalid IP address in %s: %s; skipping\n", addr.String(), ipRaw)
					break
				case ip.IsUnspecified(), ip.IsLoopback(), ip.IsInterfaceLocalMulticast():
					break
				case ip.IsLinkLocalMulticast(), ip.IsLinkLocalUnicast():
					break
				}

				// Nice, we're done!
				res = ipRaw
				ipFound = true
			} else if field == "udp" || field == "tcp" { // found port
				if res == "" {
					fmt.Printf("Possible malformed multiaddr %s; skipping\n", addr.String())
					break
				} else if i+1 >= len(fields) {
					fmt.Printf("Unexpected EOField in %s; skipping\n", addr.String())
					break
				}
				res = res + ":" + fields[i+1]
				portFound = true
				break
			}
		}

		// Nice, we found a well-formed IP+Port! Add to results:
		if ipFound && portFound {
			results = append(results, res)
		}
	}
	return results
}
