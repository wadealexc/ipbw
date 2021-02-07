package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/wadeAlexC/ipbw/crawler"
)

var (
	crawlDuration   = flag.Uint("d", 5, "(Optional) Number of minutes to crawl. 5 mins by default.")
	apiKey          = flag.String("a", "", "(REQUIRED) The API key for server authentication")
	noPublish       = flag.Bool("n", false, "(Optional) If set, will not publish results to the server.")
	publishInterval = flag.Uint("p", 1, "(Optional) Number of minutes between reports published to server. 1 min by default.")
	reportInterval  = flag.Uint("r", 1, "(Optional) Number of minutes between reports printed to console. 1 min by default.")
	numWorkers      = flag.Uint("w", 8, "(Optional) Number of goroutines to query DHT with. Default is 8.")
)

// Where to publish reports to
const server = "http://127.0.0.1:8000/batch"

// Used to check if the server is running
const serverPing = "http://127.0.0.1:8000/healthcheck"

func main() {

	// Silence that one annoying errorw from basichost.
	// lvl, err := logging.LevelFromString("DPANIC")
	// if err != nil {
	// 	panic(err)
	// }
	// logging.SetAllLoggers(lvl)

	// Parse CLI flags and make sure they make sense
	flag.Parse()

	options := &crawler.Options{
		NoPublish:       *noPublish,
		PublishInterval: *publishInterval,
		ReportInterval:  *reportInterval,
		NumWorkers:      *numWorkers,
		Server:          server,
		ServerPing:      serverPing,
		APIKey:          *apiKey,
	}

	fmt.Printf("IPBW: Starting %d minute crawl with %d workers\n", *crawlDuration, *numWorkers)
	if !*noPublish {
		fmt.Printf("Publishing results to (%s) every %d minutes\n", server, *publishInterval)
	}

	ctx := context.Background()
	crawler, err := crawler.NewCrawler(ctx, options)
	if err != nil {
		fmt.Printf("Error creating crawler: %v\nStopping...\n", err)
		return
	}
	crawler.Start()

	// Create a channel that will be notified of os.Interrupt or os.Kill signals
	// This way, if we stop the crawler (e.g. with ^C), we can exit gracefully.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)

	// Calculate total runtime
	totalDuration := time.Duration(*crawlDuration) * 60 * time.Second

	select {
	case <-time.After(totalDuration):
		crawler.Stop()
		return
	case <-ch:
		crawler.Kill()
		return
	case <-ctx.Done():
		return
	}
}
