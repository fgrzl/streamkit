package inprockit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/server"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type testNode struct {
	closed bool
}

func (n *testNode) Handle(ctx context.Context, bidi api.BidiStream) {
	bidi.Close(nil)
	n.closed = true
}
func (n *testNode) Close() {}

type testNodeManager struct {
	node server.Node
	fail bool
}

func (m *testNodeManager) GetOrCreate(ctx context.Context, storeID uuid.UUID) (server.Node, error) {
	if m.fail {
		return nil, errors.New("fail")
	}
	return m.node, nil
}
func (m *testNodeManager) Remove(ctx context.Context, storeID uuid.UUID) {}
func (m *testNodeManager) Close()                                        {}

func TestShouldRegisterStreamAndCleanupOnClose(t *testing.T) {
	// Arrange
	node := &testNode{}
	mgr := &testNodeManager{node: node}
	muxer := NewInProcMuxer(context.Background(), mgr)
	storeID := uuid.New()

	// Act
	client, err := muxer.Register(context.Background(), storeID)

	// Assert
	require.NoError(t, err)
	// Poll for stream registration with timeout
	found := false
	for i := 0; i < 100; i++ { // up to ~100ms
		muxer.mu.RLock()
		found = len(muxer.streams) > 0
		muxer.mu.RUnlock()
		if found {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	require.True(t, found, "server stream should be registered")

	// Act
	client.Close(nil)

	// Wait for cleanup
	time.Sleep(10 * time.Millisecond)

	// Assert
	muxer.mu.RLock()
	require.Len(t, muxer.streams, 0, "server stream should be removed after close")
	muxer.mu.RUnlock()
}

func TestShouldReturnServerStreamWhenRegistered(t *testing.T) {
	// Arrange
	node := &testNode{}
	mgr := &testNodeManager{node: node}
	muxer := NewInProcMuxer(context.Background(), mgr)
	storeID := uuid.New()

	// Act
	client, err := muxer.Register(context.Background(), storeID)

	// Assert
	require.NoError(t, err)
	// Poll for stream registration with timeout to avoid racing with immediate server close
	var channelID uuid.UUID
	found := false
	for i := 0; i < 100; i++ { // up to ~100ms
		muxer.mu.RLock()
		for id := range muxer.streams {
			channelID = id
			found = true
			break
		}
		muxer.mu.RUnlock()
		if found {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	require.True(t, found, "server stream should be registered")

	// Act
	server, ok := muxer.Get(channelID)

	// Assert
	require.True(t, ok)
	require.NotNil(t, server)
	client.Close(nil)
}

func TestShouldReturnErrorWhenNodeManagerFails(t *testing.T) {
	// Arrange
	mgr := &testNodeManager{fail: true}
	muxer := NewInProcMuxer(context.Background(), mgr)
	storeID := uuid.New()

	// Act
	client, err := muxer.Register(context.Background(), storeID)

	// Assert
	require.Error(t, err)
	require.Nil(t, client)
}
