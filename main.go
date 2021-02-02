package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/wadeAlexC/ipbw/crawler"

	"go.uber.org/fx"
)

var (
	crawlDuration = flag.Uint("d", 5, "(Optional) Number of minutes to crawl. 5 mins by default.")
	// noPublish       = flag.Bool("n", false, "(Optional) If set, will not publish results to the server.")
	// publishInterval = flag.Uint("p", 1, "(Optional) Number of minutes between reports published to server. 1 min by default.")
	reportInterval = flag.Uint("r", 1, "(Optional) Number of minutes between reports printed to console. 1 min by default.")
	numWorkers     = flag.Uint("w", 8, "(Optional) Number of goroutines to query DHT with. Default is 8.")
)

// Where to publish reports to
const server = "http://127.0.0.1:8000/crawl"

// Used to check if the server is running
const serverPing = "http://127.0.0.1:8000/ping"

type crawlerParams struct {
	fx.In
	Client   *crawler.Client
	Workers  *crawler.Workers
	Reporter *crawler.Reporter
}

func setOpts(params crawlerParams) error {

	// TODO server module

	var err error
	if err = params.Workers.SetNumWorkers(*numWorkers); err != nil {
		return fmt.Errorf("Error setting number of workers: %v", err)
	}

	if err = params.Reporter.SetInterval(*reportInterval); err != nil {
		return fmt.Errorf("Error setting report interval: %v", err)
	}

	return nil
}

func main() {

	// Parse CLI flags
	flag.Parse()

	app := fx.New(
		fx.Provide(
			crawler.NewClient,
			crawler.NewWorkers,
			crawler.NewReporter,
		),
		// Parse CLI flags, which sets options in the modules provided above
		fx.Invoke(setOpts),
	)

	if err := app.Start(context.Background()); err != nil {
		panic(fmt.Errorf("Error on startup: %v", err))
	}

	// Create a channel that will be notified of os.Interrupt or os.Kill signals
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)

	duration := time.Duration(*crawlDuration) * time.Minute

	select {
	case <-time.After(duration):
		if err := app.Stop(context.Background()); err != nil {
			panic(fmt.Errorf("Error on shutdown: %v", err))
		}
	case <-ch:
		if err := app.Stop(context.Background()); err != nil {
			panic(fmt.Errorf("Error on shutdown: %v", err))
		}
	}
}
