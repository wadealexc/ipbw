package main

import (
	"context"
	"fmt"

	"github.com/wadeAlexC/ipbw/crawler"
)

// const CONFIG_FILE = "config.json"

func main() {

	dht, err := crawler.NewDHT()
	if err != nil {
		fmt.Printf("error creating crawler: %v", err)
		return
	}

	err = dht.Start(context.Background())
	panic(err)
}
