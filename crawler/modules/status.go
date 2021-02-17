package modules

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	peer "github.com/libp2p/go-libp2p-peer"
	"github.com/wadeAlexC/ipbw/crawler"
	"go.uber.org/fx"
)

// Status prints updates to console
type Status struct {
	*crawler.Crawler

	crawlCancel context.CancelFunc
	crawlEvents <-chan crawler.Event

	report *sReport

	// How often status updates are printed
	interval time.Duration
	// Ctx / cancel for our worker
	sCtx    context.Context
	sCancel context.CancelFunc

	setupDone bool // Used to check that we did setup
}

type sReport struct {
	mu sync.RWMutex
	results
}

// Holds printable metrics about our crawl
type results struct {
	numReporters  uint // Number of peers that responded to our queries
	uniquePeers   uint // Number of peers that we have an ID for
	peersWithIPs  uint // Number of peers we have an IP address for
	uniqueTargets uint // Number of unique IP+port combinations we have
}

type statusParams struct {
	fx.In
	*crawler.Crawler
}

// NewStatus creates a Status that will print occasional updates to console
func NewStatus(params statusParams, lc fx.Lifecycle) (*Status, error) {

	ctx, cancel := context.WithCancel(context.Background())

	status := &Status{
		Crawler: params.Crawler,
		sCtx:    ctx,
		sCancel: cancel,
		report:  &sReport{},
	}

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			return status.start()
		},
		OnStop: func(context.Context) error {
			return status.stop()
		},
	})

	return status, nil
}

func (s *Status) Setup(interval uint) error {
	if s.setupDone {
		return fmt.Errorf("Status completed setup twice")
	} else if interval == 0 {
		return fmt.Errorf("Expected nonzero interval")
	}

	s.interval = time.Duration(interval) * time.Minute

	// Register a new listener with the crawler
	// The crawler will send us CrawlResults on the returned channel
	cancel, events := s.Crawler.Subscribe(crawler.CrawlResults)
	s.crawlCancel = cancel
	s.crawlEvents = events

	s.setupDone = true
	return nil
}

func (s *Status) start() error {
	if !s.setupDone {
		return fmt.Errorf("Expected status to be set up before start")
	}

	go s.listener()
	go s.printer()

	return nil
}

func (s *Status) stop() error {
	s.sCancel()
	// Wait briefly to give the printer a chance to print one more time
	// 5 seconds is the empirically-derived correct amount of time to wait:
	waitFor := time.Duration(5) * time.Second
	select {
	case <-time.After(waitFor):
		return nil
	}
}

func (s *Status) listener() {

	for event := range s.crawlEvents {
		// Check if we've been told to stop
		if s.sCtx.Err() != nil {
			return
		}

		if event.Type != crawler.CrawlResults {
			fmt.Printf("Expected CrawlResults, got: %v\n", event)
			continue
		}

		results := &results{}

		// Track which IDs/IPs we've seen for uniqueness tallies
		seenIP := map[string]struct{}{}
		seenID := map[peer.ID]struct{}{}

		for _, peer := range event.Result.AllPeers {
			if peer.ID == "" {
				fmt.Printf("Received empty peer ID from crawler\n")
				continue
			}

			if _, seen := seenID[peer.ID]; seen {
				fmt.Printf("Received duplicate peer %s from crawler\n", peer.ID)
				continue
			}

			seenID[peer.ID] = struct{}{}
			results.uniquePeers++

			if peer.IsReporter {
				results.numReporters++
			}

			if len(peer.Ips) != 0 {
				results.peersWithIPs++
			}

			for _, ip := range peer.Ips {
				if _, seen := seenIP[ip]; !seen {
					results.uniqueTargets++
					seenIP[ip] = struct{}{}
				}
			}
		}

		// Update report with latest results:
		s.report.mu.RLock()
		s.report.results = *results
		s.report.mu.RUnlock()
	}
}

func (s *Status) printer() {
	startTime := time.Now()
	for {
		// Check if we've been told to stop
		if s.sCtx.Err() != nil {
			return
		}

		select {
		case <-time.After(s.interval):
			timeElapsed := uint(time.Now().Sub(startTime).Minutes())
			outputArr := make([]string, 0)

			// Get results
			s.report.mu.Lock()
			results := s.report.results
			s.report.mu.Unlock()

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
