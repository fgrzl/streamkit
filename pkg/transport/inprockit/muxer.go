package inprockit

import (
	"context"
	"sync"

	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/server"
	"github.com/google/uuid"
)

type InProcMuxer struct {
	ctx         context.Context
	nodeManager server.NodeManager

	mu      sync.RWMutex
	streams map[uuid.UUID]*InProcBidiStream
}

// NewInProcMuxer initializes a test muxer with an associated nodeManager.
func NewInProcMuxer(ctx context.Context, nodeManager server.NodeManager) *InProcMuxer {
	return &InProcMuxer{
		ctx:         ctx,
		nodeManager: nodeManager,
		streams:     make(map[uuid.UUID]*InProcBidiStream),
	}
}

// Register creates a new bidirectional stream, wires it to a node, and returns the client side.
func (m *InProcMuxer) Register(ctx context.Context, storeID uuid.UUID) (api.BidiStream, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	channelID := uuid.New()
	client := NewInProcBidiStream()
	serverStream := NewInProcBidiStream()
	LinkStreams(client, serverStream)

	m.mu.Lock()
	m.streams[channelID] = serverStream
	m.mu.Unlock()

	streamCtx, cancel := context.WithCancel(ctx)
	stopBaseCancel := context.AfterFunc(m.ctx, cancel)
	streamCtx = server.WithChannelID(streamCtx, channelID)

	instance, err := m.nodeManager.GetOrCreate(streamCtx, storeID)
	if err != nil {
		stopBaseCancel()
		cancel()
		m.mu.Lock()
		delete(m.streams, channelID)
		m.mu.Unlock()
		return nil, err
	}

	go func() {
		instance.Handle(streamCtx, serverStream)
		// Optional: cleanup after stream closes
		<-serverStream.Closed()
		stopBaseCancel()
		cancel()
		m.mu.Lock()
		delete(m.streams, channelID)
		m.mu.Unlock()
	}()

	return client, nil
}

// Get returns a previously registered stream by channel ID, for test inspection.
func (m *InProcMuxer) Get(channelID uuid.UUID) (*InProcBidiStream, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	stream, ok := m.streams[channelID]
	return stream, ok
}
