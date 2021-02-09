package main

import (
	"context"
	"fmt"
	"os"

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
		Flags:                  config.Flags,
		Action: func(cctx *cli.Context) error {

			// Get application config from config file and/or flags:
			cfg, err := config.Get(cctx)
			if err != nil {
				return fmt.Errorf("Error getting config: %v", err)
			}

			// Print information about the crawl we're about to do
			cfg.PrintHello()

			fxApp := fx.New(
				fx.Options(cfg.Modules...),
				fx.Options(cfg.Invokes...),
				fx.NopLogger, // Disable fx logging. Start/Stop will short-circuit if there are errors
			)

			if err := fxApp.Start(context.Background()); err != nil {
				return fmt.Errorf("Error starting app: %v", err)
			}

			select {
			case <-fxApp.Done():
				if err := fxApp.Stop(context.Background()); err != nil {
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
