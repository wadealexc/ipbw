package main

import (
	"fmt"
	"os"

	"github.com/wadeAlexC/ipbw/crawler"

	"github.com/urfave/cli/v2"
)

// const CONFIG_FILE = "config.json"

func main() {

	app := &cli.App{
		Name:                   "interplanetary black widow",
		HelpName:               "ipbw",
		Usage:                  "crawls ur ipfs nodes",
		EnableBashCompletion:   true,
		UseShortOptionHandling: true, // combine -o + -v -> -ov
		Action: func(cctx *cli.Context) error {

			dht, err := crawler.NewDHT()
			if err != nil {
				return fmt.Errorf("error creating crawler: %v", err)
			}

			return dht.Start(cctx.Context)
		},
	}

	if err := app.Run(os.Args); err != nil {
		panic(err) // burn it all to the ground
	}
}
