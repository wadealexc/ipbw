package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/wadeAlexC/ipbw/config"

	"github.com/urfave/cli/v2"

	"go.uber.org/fx"
)

// Where to publish reports to
// const server = "http://127.0.0.1:8000/crawl"

// Used to check if the server is running
// const serverPing = "http://127.0.0.1:8000/ping"

func main() {

	app := &cli.App{
		Name:                   "Interplanetary Black Widow",
		HelpName:               "ipbw",
		Usage:                  "crawls ur IPFS nodes",
		EnableBashCompletion:   true,
		UseShortOptionHandling: true, // combine -o + -v -> -ov
		Flags: []cli.Flag{
			&cli.UintFlag{
				Name:    config.FlagCrawlDuration,
				Aliases: []string{"d"},
				Value:   5,
				Usage:   "specify the number of minutes the crawler should run for",
			},
			&cli.UintFlag{
				Name:    config.FlagNumWorkers,
				Aliases: []string{"n"},
				Value:   8,
				Usage:   "specify the number of goroutines used to query the DHT",
			},
			&cli.BoolFlag{
				Name:    config.FlagEnableStatus,
				Aliases: []string{"s"},
				Value:   true,
				Usage:   "specify whether the crawler should output occasional status updates to console.",
			},
			&cli.UintFlag{
				Name:    config.FlagStatusInterval,
				Aliases: []string{"si"},
				Value:   1,
				Usage:   "specify how often (in minutes) status updates will be posted",
			},
			&cli.BoolFlag{
				Name:    config.FlagEnableIdentifier,
				Aliases: []string{"i"},
				Value:   false,
				Usage:   "enables the identifier module",
			},
		},
		Action: func(cctx *cli.Context) error {

			// Get default crawler config from cli flags
			cfg := config.Default(cctx)

			// Get config for each module from cli flags:
			cfg.ConfigStatus(cctx)     // modules/status
			cfg.ConfigIdentifier(cctx) // modules/identifier

			// Print information about the crawl we're about to do
			cfg.Hello()

			app := fx.New(
				fx.Options(cfg.Modules...),
				fx.Options(cfg.Invokes...),
				fx.NopLogger, // Disable fx logging. Start/Stop will short-circuit if there are errors
			)

			// Get crawl duration:
			duration := time.Duration(cctx.Uint(config.FlagCrawlDuration)) * time.Minute

			fmt.Printf("Starting crawl...\n")

			if err := app.Start(context.Background()); err != nil {
				return fmt.Errorf("Error starting app: %v", err)
			}

			// Create a channel that will be notified of os.Interrupt or os.Kill signals
			ch := make(chan os.Signal, 1)
			signal.Notify(ch, os.Interrupt, os.Kill)

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

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		panic(err) // burn it all to the ground
	}
}
