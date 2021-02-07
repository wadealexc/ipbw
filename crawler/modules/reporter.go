package modules

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	peer "github.com/libp2p/go-libp2p-peer"
	"github.com/wadeAlexC/ipbw/crawler"
	"go.uber.org/fx"
)

type Reporter struct {
	*crawler.Crawler

	crawlCancel context.CancelFunc
	crawlEvents <-chan crawler.Event

	report *rReport

	rCtx    context.Context
	rCancel context.CancelFunc

	interval        time.Duration
	publishEndpoint string
	pingEndpoint    string
	apiKey          string

	setupDone bool // Used to check that we did setup
}

type rReport struct {
	mu sync.RWMutex
	*ReportJSON
}

// PeerJSON is a json-marshallable form of Peer
type PeerJSON struct {
	Pid       string   `json:"pid"`
	Ips       []string `json:"ips"`
	Neighbors []string `json:"neighbors"`
	Timestamp string   `json:"timestamp"`
}

type ReportJSON struct {
	Peers []PeerJSON `json:"peers"`
}

type reporterParams struct {
	fx.In
	*crawler.Crawler
}

func NewReporter(params reporterParams, lc fx.Lifecycle) (*Reporter, error) {

	ctx, cancel := context.WithCancel(context.Background())

	reporter := &Reporter{
		Crawler: params.Crawler,
		rCtx:    ctx,
		rCancel: cancel,
		report: &rReport{
			ReportJSON: &ReportJSON{
				Peers: make([]PeerJSON, 0),
			},
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

func (r *Reporter) Setup(interval uint, publishEndpoint string, pingEndpoint string, apiKey string) error {
	if r.setupDone {
		return fmt.Errorf("Reporter completed setup twice")
	} else if interval == 0 {
		return fmt.Errorf("Expected nonzero report interval")
	} else if publishEndpoint == "" {
		return fmt.Errorf("Expected nonempty publishEndpoint")
	} else if pingEndpoint == "" {
		return fmt.Errorf("Expected nonempty pingEndpoint")
	} else if apiKey == "" {
		return fmt.Errorf("Expected nonempty API key")
	}

	r.interval = time.Duration(interval) * time.Minute
	r.publishEndpoint = publishEndpoint
	r.pingEndpoint = pingEndpoint
	r.apiKey = apiKey

	// Register a new listener with the crawler
	// The crawler will send us CrawlResults on the returned channel
	cancel, events := r.Crawler.NewListener(crawler.CrawlResults)
	r.crawlCancel = cancel
	r.crawlEvents = events

	r.setupDone = true
	return nil
}

func (r *Reporter) start() error {
	if !r.setupDone {
		return fmt.Errorf("Expected reporter to be set up before start")
	}

	// Make sure server is running
	response, err := r.pingServer()
	if err != nil {
		return err
	}

	fmt.Printf("Server says: %s\n", response)

	go r.listener()
	go r.reporter()
	return nil
}

func (r *Reporter) stop() error {
	r.rCancel()
	r.crawlCancel()

	// Wait briefly to give the crawler a chance to shut down
	// 5 seconds is the empirically-derived correct amount of time to wait:
	waitFor := time.Duration(5) * time.Second
	select {
	case <-time.After(waitFor):
		return nil
	}
}

func (r *Reporter) listener() {

	for event := range r.crawlEvents {
		// Check if we've been told to stop
		if r.rCtx.Err() != nil {
			return
		}

		if event.Type != crawler.CrawlResults {
			fmt.Printf("Expected CrawlResults, got: %v\n", event)
			continue
		}

		seenID := map[peer.ID]struct{}{}
		report := &ReportJSON{
			Peers: make([]PeerJSON, 0),
		}

		// Iterate over peers and convert to ReportJSON
		for _, peer := range event.Result.AllPeers {
			if _, seen := seenID[peer.ID]; seen {
				fmt.Printf("Duplicate peer in crawler result: %s\n", peer.ID)
				continue
			}

			seenID[peer.ID] = struct{}{}
			neighborStrings := make([]string, len(peer.Neighbors))

			// Convert neighbor IDs to strings
			for _, neighbor := range peer.Neighbors {
				neighborStrings = append(neighborStrings, neighbor.Pretty())
			}

			// Convert peer to PeerJSON
			pJSON := PeerJSON{
				Pid:       peer.ID.Pretty(),
				Ips:       peer.Ips,
				Neighbors: neighborStrings,
				Timestamp: peer.Timestamp,
			}

			report.Peers = append(report.Peers, pJSON)
		}

		// Update report
		r.report.mu.RLock()
		r.report.ReportJSON = report
		r.report.mu.RUnlock()
	}
}

func (r *Reporter) reporter() {

	for {
		// Check if we've been told to stop
		if r.rCtx.Err() != nil {
			return
		}

		select {
		case <-time.After(r.interval):

			r.report.mu.Lock()
			reportBody, err := json.Marshal(r.report.ReportJSON)
			if err != nil {
				fmt.Printf("Error marshalling ReportJSON: %v\n", err)
				continue
			}
			r.report.mu.Unlock()

			client := &http.Client{}

			req, err := http.NewRequest("POST", r.publishEndpoint, bytes.NewBuffer(reportBody))
			if err != nil {
				fmt.Printf("Error creating POST request: %v\n", err)
				continue
			}

			req.Header.Add("User-Agent", fmt.Sprintf("ipbw-go-%s", crawler.Version))
			req.Header.Add("Authorization", r.apiKey)

			resp, err := client.Do(req)
			if err != nil {
				fmt.Printf("Error publishing to server: %v\n", err)
				continue
			}

			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				fmt.Printf("Error reading server response: %v\n", err)
				continue
			}

			fmt.Printf("Published report to server. Got response: %s\n", string(body))
			resp.Body.Close()
		}
	}
}

func (r *Reporter) pingServer() (string, error) {
	client := &http.Client{}

	req, err := http.NewRequest("GET", r.pingEndpoint, bytes.NewBuffer(nil))
	if err != nil {
		return "", fmt.Errorf("Error constructing GET request: %v", err)
	}

	req.Header.Add("User-Agent", fmt.Sprintf("ipbw-go-%s", crawler.Version))
	req.Header.Add("Authorization", r.apiKey)

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("Got error pinging server: %v", err)
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("Got error decoding server response: %v", err)
	}

	return string(body), nil
}
