package modules

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	peer "github.com/libp2p/go-libp2p-peer"
	"github.com/wadeAlexC/ipbw/crawler"
	"go.uber.org/fx"
)

type Identifier struct {
	*crawler.Crawler

	// Events / cancel used to listen to crawler
	crawlCancel context.CancelFunc
	crawlEvents <-chan crawler.Event

	// Ctx / cancel for our workers
	iCtx    context.Context
	iCancel context.CancelFunc

	interval time.Duration
	report   *iReport
	supports map[peer.ID][]string

	setupDone bool // Used to check that we did setup
}

type iReport struct {
	mu    sync.RWMutex
	peers []peer.ID
}

type identifierParams struct {
	fx.In
	*crawler.Crawler
}

func NewIdentifier(params identifierParams, lc fx.Lifecycle) (*Identifier, error) {

	ctx, cancel := context.WithCancel(context.Background())

	i := &Identifier{
		Crawler: params.Crawler,
		iCtx:    ctx,
		iCancel: cancel,
		report: &iReport{
			peers: make([]peer.ID, 0),
		},
		supports: make(map[peer.ID][]string),
	}

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			return i.start()
		},
		OnStop: func(context.Context) error {
			return i.stop()
		},
	})

	return i, nil
}

func (i *Identifier) Setup(interval uint) error {
	if i.setupDone {
		return fmt.Errorf("Identifier completed setup twice")
	} else if interval == 0 {
		return fmt.Errorf("Expected nonzero interval")
	}

	// Register a new listener with the crawler
	// The crawler will send us new peers on the returned channel
	cancel, events := i.Crawler.NewListener(crawler.NewPeers)
	i.crawlCancel = cancel
	i.crawlEvents = events

	i.interval = time.Duration(interval) * time.Minute

	i.setupDone = true
	return nil
}

func (i *Identifier) start() error {
	if !i.setupDone {
		return fmt.Errorf("Expected identifier to be set up before start")
	}

	go i.listener()
	go i.aggregator()
	return nil
}

func (i *Identifier) stop() error {
	i.iCancel()
	i.crawlCancel()

	// Wait briefly to give the crawler a chance to shut down
	// 5 seconds is the empirically-derived correct amount of time to wait:
	waitFor := time.Duration(5) * time.Second
	select {
	case <-time.After(waitFor):
		return nil
	}
}

// Collects new peers from the crawler
func (i *Identifier) listener() {

	seenID := map[peer.ID]struct{}{}

	for event := range i.crawlEvents {
		// Check if we've been told to stop
		if i.iCtx.Err() != nil {
			return
		}

		if event.Type != crawler.NewPeers {
			fmt.Printf("Expected NewPeers, got: %v\n", event)
			continue
		}

		// Lock reads while we update report
		i.report.mu.RLock()

		// Iterate over received peers and add to report
		for _, peer := range event.Result.NewPeers {
			if _, seen := seenID[peer]; seen {
				fmt.Printf("Crawler claims peer %s is new, but it's a lie!\n", peer)
				continue
			}

			i.report.peers = append(i.report.peers, peer)
		}

		i.report.mu.RUnlock()
	}
}

// Aggregates protocols supported by peers we've collected from the crawler
func (i *Identifier) aggregator() {
	for {
		// Check if we've been told to stop
		if i.iCtx.Err() != nil {
			return
		}

		select {
		case <-time.After(i.interval):
			results := i.queryProtocols()
			i.printUpdate(results)
		}
	}
}

type iResults struct {
	mostProtocols peer.ID         // Peer that supports the most protocols
	mostSupported int             // The number of protocols they support
	seenProtos    map[string]uint // map[protocol] -> # peers supporting
}

func (i *Identifier) queryProtocols() *iResults {

	results := &iResults{
		mostSupported: -1,
		seenProtos:    make(map[string]uint),
	}

	// Lock writes while we read from the report
	i.report.mu.Lock()
	// Iterate over all reported peers and check our peerstore for each
	for _, peer := range i.report.peers {
		protos, err := i.Crawler.PS.GetProtocols(peer)

		if err != nil {
			fmt.Printf("Got error querying protocols for peer %s: %v; skipping\n", peer, err)
			continue
		}

		if len(protos) == 0 {
			continue
		}

		// Create entry if it doesn't exist
		// if _, exists := i.supports[peer]; !exists {
		// 	i.supports[peer] = make([]string, 0)
		// }

		// Figure out what protocols we've already seen for this peer
		seenProto := map[string]struct{}{}
		for _, proto := range i.supports[peer] {
			seenProto[proto] = struct{}{}
		}

		// Now, create a new supported list given the latest query
		for _, proto := range protos {
			if _, seen := seenProto[proto]; !seen {
				seenProto[proto] = struct{}{}
				i.supports[peer] = append(i.supports[peer], proto)
			}
		}
	}
	i.report.mu.Unlock()

	for peer, protos := range i.supports {

		// Update mostProtocols
		if len(protos) > results.mostSupported {
			results.mostProtocols = peer
			results.mostSupported = len(protos)
		}

		for _, proto := range protos {
			results.seenProtos[proto]++
		}
	}

	return results
}

type protoPair struct {
	proto string
	count uint
}

type pairList []protoPair

func (i *Identifier) printUpdate(res *iResults) {

	outputArr := make([]string, 0)

	outputArr = append(outputArr, fmt.Sprintf("--- Identifier results: ---\n"))
	outputArr = append(outputArr, fmt.Sprintf(">> %d peers queried:\n", len(i.supports)))
	outputArr = append(outputArr, fmt.Sprintf("- %d unique protocols supported\n", len(res.seenProtos)))
	outputArr = append(outputArr, fmt.Sprintf("- Peer %s supports the most, at %d\n", res.mostProtocols, res.mostSupported))
	outputArr = append(outputArr, fmt.Sprintf("- Protocols supported:\n"))

	list := make(pairList, len(res.seenProtos))
	idx := 0
	for proto, count := range res.seenProtos {
		list[idx] = protoPair{proto, count}
		idx++
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].count > list[j].count
	})

	for _, entry := range list {
		outputArr = append(outputArr, fmt.Sprintf("%s -> %d peers\n", entry.proto, entry.count))
	}

	output := strings.Join(outputArr, "")
	fmt.Printf(output)
}
