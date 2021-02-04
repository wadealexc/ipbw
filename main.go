package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/wadeAlexC/ipbw/crawler"
	"github.com/wadeAlexC/ipbw/crawler/modules"

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
				Name:    "duration",
				Aliases: []string{"d"},
				Value:   5,
				Usage:   "specify the number of minutes the crawler should run for",
			},
			&cli.UintFlag{
				Name:    "num-workers",
				Aliases: []string{"n"},
				Value:   8,
				Usage:   "specify the number of goroutines used to query the DHT",
			},
			&cli.BoolFlag{
				Name:    "output-status",
				Aliases: []string{"o"},
				Value:   true,
				Usage:   "specify whether the crawler should output occasional status updates to console.",
			},
			&cli.UintFlag{
				Name:    "output-interval",
				Aliases: []string{"i"},
				Value:   1,
				Usage:   "specify how often (in minutes) status updates will be posted",
			},
			&cli.BoolFlag{
				Name:    "enhanced-interrogation",
				Aliases: []string{"e"},
				Value:   false,
				Usage:   "enables the interrogator module",
			},
		},
		Action: func(cctx *cli.Context) error {

			settings := &settings{
				modules: make([]fx.Option, 0),
				invokes: make([]fx.Option, 0),
			}

			settings.modules = append(settings.modules, fx.Provide(crawler.NewCrawler))
			settings.invokes = append(settings.invokes, fx.Invoke(func(c *crawler.Crawler) error {
				return c.SetNumWorkers(cctx.Uint("num-workers"))
			}))

			enableOutput := cctx.Bool("output-status")
			if enableOutput {
				settings.modules = append(settings.modules, fx.Provide(modules.NewOutput))
				settings.invokes = append(settings.invokes, fx.Invoke(func(o *modules.Output) error {
					return o.SetInterval(cctx.Uint("output-interval"))
				}))
			}

			if cctx.Bool("enhanced-interrogation") {
				settings.modules = append(settings.modules, fx.Provide(modules.NewInterrogator))
				settings.invokes = append(settings.invokes, fx.Invoke(func(i *modules.Interrogator) error {
					return i.Subscribe()
				}))
			}

			app := fx.New(
				fx.Options(settings.modules...),
				fx.Options(settings.invokes...),
				fx.NopLogger, // Disable fx logging. Start/Stop will short-circuit if there are errors
			)

			fmt.Printf("Starting crawl with %d workers for %d minutes\n", cctx.Uint("num-workers"), cctx.Uint("duration"))

			// Get crawl duration:
			duration := time.Duration(cctx.Uint("duration")) * time.Minute

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

func defaults() []fx.Option {
	return nil
}

type settings struct {
	modules []fx.Option
	invokes []fx.Option
}

type option func(*settings) error

func options(opts ...option) option {
	return func(s *settings) error {
		for _, opt := range opts {
			if err := opt(s); err != nil {
				return err
			}
		}
		return nil
	}
}
