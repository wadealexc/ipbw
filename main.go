package main

import (
	"fmt"
	"os"

	"github.com/wadeAlexC/ipbw/crawler"

	"github.com/urfave/cli/v2"
)

func main() {

	app := &cli.App{
		Name:                   "Interplanetary Black Widow",
		HelpName:               "ipbw",
		EnableBashCompletion:   true,
		UseShortOptionHandling: true,
		Flags: []cli.Flag{
			&cli.UintFlag{
				Name:    "duration",
				Aliases: []string{"d"},
				Value:   0,
				Usage:   "Specify the `NUM_MINUTES` to run the crawler, or 0 for endless mode.",
			},
		},
		Action: func(cctx *cli.Context) error {
			dht, err := crawler.NewDHT(cctx.Uint("duration"))
			if err != nil {
				return fmt.Errorf("error creating crawler: %v", err)
			}

			dht.Start()
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		panic(err) // burn it all to the ground
	}
}
