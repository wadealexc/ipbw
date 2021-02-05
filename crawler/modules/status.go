package modules

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/wadeAlexC/ipbw/crawler"
	"go.uber.org/fx"
)

// Status prints updates to console
type Status struct {
	*crawler.Crawler

	// How often status updates are printed
	interval time.Duration
	// Ctx / cancel for our worker
	printerCtx    context.Context
	printerCancel context.CancelFunc
}

type statusParams struct {
	fx.In
	*crawler.Crawler
}

// NewStatus creates a Status that will print occasional updates to console
func NewStatus(params statusParams, lc fx.Lifecycle) (*Status, error) {

	ctx, cancel := context.WithCancel(context.Background())

	status := &Status{
		Crawler:       params.Crawler,
		printerCtx:    ctx,
		printerCancel: cancel,
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

// SetInterval sets the duration between status updates printed to console
// interval should be a number of minutes, which is converted to time.Duration here
func (s *Status) SetInterval(interval uint) error {
	if interval == 0 {
		return fmt.Errorf("Expected nonzero interval")
	}
	s.interval = time.Duration(interval) * time.Minute
	return nil
}

func (s *Status) start() error {
	if s.interval == 0 {
		return fmt.Errorf("Interval not set")
	}

	go s.printer()

	return nil
}

func (s *Status) stop() error {
	fmt.Printf("Stopping status...\n")

	s.printerCancel()
	// Wait briefly to give the printer a chance to print one more time
	// 5 seconds is the empirically-derived correct amount of time to wait:
	waitFor := time.Duration(5) * time.Second
	select {
	case <-time.After(waitFor):
		return nil
	}
}

// Holds printable metrics about our crawl
type results struct {
	numReporters  uint // Number of peers that responded to our queries
	uniquePeers   uint // Number of peers that we have an ID for
	peersWithIPs  uint // Number of peers we have an IP address for
	uniqueTargets uint // Number of unique IP+port combinations we have
}

func (s *Status) printer() {
	startTime := time.Now()
	for {
		// Check if we've been told to stop
		if s.printerCtx.Err() != nil {
			return
		}

		select {
		case <-time.After(s.interval):
			timeElapsed := uint(time.Now().Sub(startTime).Minutes())

			// Tally results
			results := s.getResults()

			outputArr := make([]string, 0)
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

// Iterates over our report and retrieves printable metrics
func (s *Status) getResults() *results {

	s.Crawler.Report.Mu.Lock()
	defer s.Crawler.Report.Mu.Unlock()

	results := &results{
		uniquePeers: uint(len(s.Crawler.Report.Peers)),
	}

	// Track which IPs we've seen for unique target tally
	seenIP := map[string]struct{}{}

	for _, peer := range s.Crawler.Report.Peers {
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

	return results
}
