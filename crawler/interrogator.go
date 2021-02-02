package crawler

import (
	"context"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"go.uber.org/fx"
)

type Interrogator struct {
	*Reporter
	ps peerstore.Peerstore

	interrogated map[peer.ID]bool
}

type interrogatorParams struct {
	fx.In
	Reporter *Reporter
}

func NewInterrogator(params interrogatorParams, lc fx.Lifecycle) (*Interrogator, error) {

	i := &Interrogator{
		Reporter: params.Reporter,
		ps:       params.Reporter.Client.Host.Peerstore(),
	}

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			return i.start()
		},
	})

	return i, nil
}

func (i *Interrogator) start() error {
	go i.queryProtocols()
	return nil
}

func (i *Interrogator) queryProtocols() {
	i.Reporter.Report.mu.Lock()

	count := len(i.Reporter.Report.peers)

	defer i.Reporter.Report.mu.Unlock()
}

func (i *Interrogator) getPeerProtocols(id peer.ID) ([]string, error) {
	return i.ps.GetProtocols(id)
}
