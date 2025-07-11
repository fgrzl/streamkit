package mockkit

import (
	"context"
	"sync"

	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/node"
	"github.com/google/uuid"
)

type MockMuxer struct {
	ctx         context.Context
	nodeManager node.NodeManager

	mu      sync.RWMutex
	streams map[uuid.UUID]*MockBidiStream
}

// NewMockMuxer initializes a test muxer with an associated nodeManager.
func NewMockMuxer(ctx context.Context, nodeManager node.NodeManager) *MockMuxer {
	return &MockMuxer{
		ctx:         ctx,
		nodeManager: nodeManager,
		streams:     make(map[uuid.UUID]*MockBidiStream),
	}
}

// Register creates a new bidirectional stream, wires it to a node, and returns the client side.
func (m *MockMuxer) Register(storeID uuid.UUID) (api.BidiStream, error) {
	channelID := uuid.New()
	client := NewMockBidiStream()
	server := NewMockBidiStream()
	LinkStreams(client, server)

	m.mu.Lock()
	m.streams[channelID] = server
	m.mu.Unlock()

	ctx := node.WithChannelID(m.ctx, channelID)

	instance, err := m.nodeManager.GetOrCreate(ctx, storeID)
	if err != nil {
		return nil, err
	}

	go func() {
		instance.Handle(ctx, server)
		// Optional: cleanup after stream closes
		<-server.Closed()
		m.mu.Lock()
		delete(m.streams, channelID)
		m.mu.Unlock()
	}()

	return client, nil
}

// Get returns a previously registered stream by channel ID, for test inspection.
func (m *MockMuxer) Get(channelID uuid.UUID) (*MockBidiStream, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	stream, ok := m.streams[channelID]
	return stream, ok
}
