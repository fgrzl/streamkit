// Package inprockit provides in-memory in-process implementations of transport
// interfaces for testing and development purposes.
//
// This package implements bidirectional streaming without network transport,
// allowing for fast unit testing and local development of streamkit applications.
package inprockit

import (
	"context"

	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/server"
	"github.com/google/uuid"
)

// InProcBidiStreamPair represents a connected pair of in-process bidirectional streams.
type InProcBidiStreamPair struct {
	Client *InProcBidiStream
	Server *InProcBidiStream
}

// InProcBidiStreamProvider provides in-process bidirectional streams for testing and local use.
type InProcBidiStreamProvider struct {
	muxer *InProcMuxer
}

// NewInProcBidiStreamProvider initializes the provider with a backing InProcMuxer.
func NewInProcBidiStreamProvider(ctx context.Context, nm server.NodeManager) *InProcBidiStreamProvider {
	return &InProcBidiStreamProvider{
		muxer: NewInProcMuxer(ctx, nm),
	}
}

// CallStream creates a new logical stream via the muxer and records the client/server pair.
func (p *InProcBidiStreamProvider) CallStream(
	ctx context.Context,
	storeID uuid.UUID,
	routeable api.Routeable,
) (api.BidiStream, error) {
	client, err := p.muxer.Register(storeID)
	if err != nil {
		return nil, err
	}

	envelope := polymorphic.NewEnvelope(routeable)
	if err := client.Encode(envelope); err != nil {
		client.Close(err)
		return nil, err
	}

	return client, nil
}
