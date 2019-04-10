package pubsub

import (
	"context"

	"github.com/devopsfaith/krakend/config"
	"github.com/devopsfaith/krakend/logging"
	"github.com/devopsfaith/krakend/proxy"
)

func NewBackendFactory(ctx context.Context, logger logging.Logger, bf proxy.BackendFactory) proxy.BackendFactory {
	f := backendFactory{
		logger: logger,
		bf:     bf,
		ctx:    ctx,
	}

	return f.New
}

type backendFactory struct {
	ctx    context.Context
	logger logging.Logger
	bf     proxy.BackendFactory
}

func (f backendFactory) New(remote *config.Backend) proxy.Proxy {
	if prxy, err := f.initSubscriber(f.ctx, remote); err == nil {
		return prxy
	}

	if prxy, err := f.initPublisher(f.ctx, remote); err == nil {
		return prxy
	}

	return f.bf(remote)
}
