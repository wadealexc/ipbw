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
				Usage:   "how often (in minutes) status updates will be posted",
			},
			&cli.BoolFlag{
				Name:    config.FlagEnableIdentifier,
				Aliases: []string{"i"},
				Value:   false,
				Usage:   "enables the identifier module",
			},
			&cli.BoolFlag{
				Name:    config.FlagEnableReporter,
				Aliases: []string{"r"},
				Value:   false,
				Usage:   "enables the reporter module, which publishes results to a server",
			},
			&cli.UintFlag{
				Name:    config.FlagReportInterval,
				Aliases: []string{"ri"},
				Value:   1,
				Usage:   "how often (in minutes) the reporter will publish crawl results to the server",
			},
			&cli.StringFlag{
				Name:    config.FlagReportPublishEndpoint,
				Aliases: []string{"re"},
				Value:   "http://127.0.0.1:8000/batch",
				Usage:   "the url/endpoint the reporter will publish crawl results to",
			},
			&cli.StringFlag{
				Name:    config.FlagReportPingEndpoint,
				Aliases: []string{"rp"},
				Value:   "http://127.0.0.1:8000/healthcheck",
				Usage:   "the url/endpoint the reporter will ping on startup to ensure the server is running",
			},
			&cli.StringFlag{
				Name:    config.FlagReportAPIKey,
				Aliases: []string{"ra"},
				Usage:   "the API key used to authenticate the reporter's reports",
			},
		},
		Action: func(cctx *cli.Context) error {

			// Get default crawler config from cli flags:
			cfg := config.Default(cctx)

			// Get config for optional modules, if enabled:
			cfg.ConfigStatus(cctx)     // modules/status
			cfg.ConfigIdentifier(cctx) // modules/identifier
			cfg.ConfigReporter(cctx)   // modules/reporter

			// Print information about the crawl we're about to do
			cfg.PrintHello()

			app := fx.New(
				fx.Options(cfg.Modules...),
				fx.Options(cfg.Invokes...),
				fx.NopLogger, // Disable fx logging. Start/Stop will short-circuit if there are errors
			)

			// Get crawl duration:
			duration := time.Duration(cctx.Uint(config.FlagCrawlDuration)) * time.Minute

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
