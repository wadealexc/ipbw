package crawler

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	mh "github.com/multiformats/go-multihash"
	"go.uber.org/fx"
)

// Workers execute queries on the DHT
type Workers struct {
	*Client

	NumWorkers uint
}

type workersParams struct {
	fx.In
	Client *Client
}

// NewWorkers instantiates our Workers and defines OnStart/OnStop hooks
func NewWorkers(params workersParams, lc fx.Lifecycle) (*Workers, error) {

	workers := &Workers{
		Client: params.Client,
	}

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			fmt.Printf("Starting %d workers\n", workers.NumWorkers)
			workers.start()
			return nil
		},
		OnStop: func(context.Context) error {
			// workers.stop()
			return nil
		},
	})

	return workers, nil
}

// SetNumWorkers sets the number of goroutines that will be created to query the DHT
func (w *Workers) SetNumWorkers(numWorkers uint) error {
	if numWorkers == 0 {
		return fmt.Errorf("Expected nonzero number of workers")
	}
	w.NumWorkers = numWorkers
	return nil
}

func (w *Workers) start() {
	for i := 0; i < int(w.NumWorkers); i++ {
		go w.run()
	}
}

func (w *Workers) run() {
Work:
	id, err := randPeerID()
	if err != nil {
		fmt.Printf("Error getting random peer ID: %v\n", err)
		goto Work
	}

	ctx, cancel := context.WithTimeout(w.Client.Ctx, 10*time.Second)
	_, _ = w.Client.DHT.FindPeer(ctx, id)
	cancel()
	goto Work
}

// func (w *Worker) stop() {}

func randPeerID() (peer.ID, error) {
	buf := make([]byte, 16)
	rand.Read(buf)
	h, err := mh.Sum(buf, mh.SHA2_256, -1)
	if err != nil {
		return "", err
	}
	return peer.ID(h), nil
}
