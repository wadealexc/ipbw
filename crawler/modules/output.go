package modules

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/wadeAlexC/ipbw/crawler"
	"go.uber.org/fx"
)

// Output prints status updates to console
type Output struct {
	*crawler.Crawler
	Interval      time.Duration
	PrinterCtx    context.Context
	PrinterCancel context.CancelFunc
}

type outputParams struct {
	fx.In
	*crawler.Crawler
}

// NewOutput creates an Output that will print occasional status updates to console
func NewOutput(params outputParams, lc fx.Lifecycle) (*Output, error) {

	ctx, cancel := context.WithCancel(context.Background())

	output := &Output{
		Crawler:       params.Crawler,
		PrinterCtx:    ctx,
		PrinterCancel: cancel,
	}

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			return output.start()
		},
		OnStop: func(context.Context) error {
			return output.stop()
		},
	})

	return output, nil
}

// SetInterval sets the duration between status updates printed to console
// interval should be a number of minutes, which is converted to time.Duration here
func (o *Output) SetInterval(interval uint) error {
	if interval == 0 {
		return fmt.Errorf("Expected nonzero interval")
	}
	o.Interval = time.Duration(interval) * time.Minute
	return nil
}

func (o *Output) start() error {
	if o.Interval == 0 {
		return fmt.Errorf("Interval not set")
	}

	go o.printer()

	return nil
}

func (o *Output) stop() error {
	fmt.Printf("Stopping output...\n")

	o.PrinterCancel()
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

func (o *Output) printer() {
	startTime := time.Now()
	for {
		// Check if we've been told to stop
		if o.PrinterCtx.Err() != nil {
			return
		}

		select {
		case <-time.After(o.Interval):
			timeElapsed := uint(time.Now().Sub(startTime).Minutes())

			// Tally results
			results := o.getResults()

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
func (o *Output) getResults() *results {

	o.Crawler.Report.Mu.Lock()
	defer o.Crawler.Report.Mu.Unlock()

	results := &results{
		uniquePeers: uint(len(o.Crawler.Report.Peers)),
	}

	// Track which IPs we've seen for unique target tally
	seenIP := map[string]struct{}{}

	for _, peer := range o.Crawler.Report.Peers {
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
