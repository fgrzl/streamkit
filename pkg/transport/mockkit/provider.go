// Package mockkit provides in-memory mock implementations of transport
// interfaces for testing and development purposes.
//
// This package implements bidirectional streaming without network transport,
// allowing for fast unit testing and local development of streamkit applications.
package mockkit

import (
	"context"

	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/node"
	"github.com/google/uuid"
)

// MockBidiStreamPair represents a connected pair of mock bidirectional streams.
type MockBidiStreamPair struct {
	Client *MockBidiStream
	Server *MockBidiStream
}

// MockBidiStreamProvider provides mock bidirectional streams for testing.
type MockBidiStreamProvider struct {
	muxer *MockMuxer
}

// NewMockBidiStreamProvider initializes the provider with a backing MockMuxer.
func NewMockBidiStreamProvider(ctx context.Context, nm node.NodeManager) *MockBidiStreamProvider {
	return &MockBidiStreamProvider{
		muxer: NewMockMuxer(ctx, nm),
	}
}

// CallStream creates a new logical stream via the muxer and records the client/server pair.
func (p *MockBidiStreamProvider) CallStream(
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
