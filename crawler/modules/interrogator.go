package modules

import (
	"context"

	"github.com/wadeAlexC/ipbw/crawler"
	"go.uber.org/fx"
)

// Interrogator interrogates various suspects
type Interrogator struct {
	*crawler.Crawler

	Ctx    context.Context
	Cancel context.CancelFunc
}

type interrogatorParams struct {
	fx.In
	*crawler.Crawler
}

// NewInterrogator creates an interrogator
func NewInterrogator(params interrogatorParams, lc fx.Lifecycle) (*Interrogator, error) {

	ctx, cancel := context.WithCancel(context.Background())

	i := &Interrogator{
		Crawler: params.Crawler,
		Ctx:     ctx,
		Cancel:  cancel,
	}

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			return i.start()
		},
		OnStop: func(context.Context) error {
			return i.stop()
		},
	})

	return i, nil
}

func (i *Interrogator) start() error {
	return nil
}

func (i *Interrogator) stop() error {
	return nil
}
