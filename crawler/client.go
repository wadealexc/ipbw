package crawler

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-datastore"
	badger "github.com/ipfs/go-ds-badger"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	"go.uber.org/fx"
)

// Client is the basic building block of our crawler. It contains:
// 1. Various libp2p / ipfs fields
// 2. An Events channel, which will receive QueryEvents from the DHT
type Client struct {
	Host host.Host
	DHT  *dht.IpfsDHT
	DS   datastore.Batching
	ID   *identify.IDService

	// We receive QueryEvents from the DHT on this channel
	Events <-chan *routing.QueryEvent
	Ctx    context.Context
	Cancel context.CancelFunc
}

type clientParams struct {
	fx.In
}

// NewClient instantiates our client, libp2p node, dht database, and more
func NewClient(params clientParams, lc fx.Lifecycle) (*Client, error) {

	// Start a libp2p node
	host, err := libp2p.New(context.Background())
	if err != nil {
		return nil, fmt.Errorf("Error creating libp2p node: %v", err)
	}

	// Create a DB for our DHT client
	ds, err := badger.NewDatastore("dht.db", nil)
	if err != nil {
		return nil, fmt.Errorf("Error getting datastore: %v", err)
	}

	// Create a context that, when passed to DHT queries, emits QueryEvents to a channel
	ctx, cancel := context.WithCancel(context.Background())
	ctx, events := routing.RegisterForQueryEvents(ctx)

	client := &Client{
		Host:   host,
		DHT:    dht.NewDHTClient(ctx, host, ds),
		DS:     ds,
		ID:     identify.NewIDService(host),
		Ctx:    ctx,
		Cancel: cancel,
		Events: events,
	}

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			return client.start()
		},
		OnStop: func(context.Context) error {
			return client.stop()
		},
	})

	return client, nil
}

func (c *Client) start() error {
	// Bootstrap
	for _, a := range dht.DefaultBootstrapPeers {
		pInfo, err := peer.AddrInfoFromP2pAddr(a)
		if err != nil {
			return fmt.Errorf("Error getting bootstrap AddrInfo: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err = c.Host.Connect(ctx, *pInfo); err != nil {
			fmt.Printf("Skipping bootstrap peer %s; received error: %v\n", pInfo.ID.Pretty(), err)
		}

		cancel()
	}

	return nil
}

func (c *Client) stop() error {
	c.Cancel()
	return nil
}
