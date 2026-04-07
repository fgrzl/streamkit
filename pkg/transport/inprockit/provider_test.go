package inprockit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/server"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeNode captures the first decoded envelope content sent to Handle
type fakeNode struct {
	received interface{}
	done     chan struct{}
}

func (f *fakeNode) Handle(ctx context.Context, bidi api.BidiStream) {
	env := &polymorphic.Envelope{}
	_ = bidi.Decode(env)
	f.received = env.Content
	if f.done != nil {
		close(f.done)
	}
	bidi.Close(nil)
}

func (f *fakeNode) Close() {}

type fakeNodeManager struct {
	node server.Node
	err  error
}

func (m *fakeNodeManager) GetOrCreate(ctx context.Context, storeID uuid.UUID) (server.Node, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.node, nil
}

func (m *fakeNodeManager) Remove(ctx context.Context, storeID uuid.UUID) {}
func (m *fakeNodeManager) Close()                                        {}

type fakeReconnectListener struct {
	calls int
}

func (l *fakeReconnectListener) OnReconnected(ctx context.Context, storeID uuid.UUID) error {
	l.calls++
	return nil
}

func nilContextForTest() context.Context {
	var ctx context.Context
	return ctx
}

func TestShouldDeliverEnvelopeToNodeWhenCallStreamInvoked(t *testing.T) {
	// Arrange
	fnode := &fakeNode{done: make(chan struct{})}
	nm := &fakeNodeManager{node: fnode}
	provider := NewInProcBidiStreamProvider(context.Background(), nm)

	// Act
	route := &api.GetSpaces{}
	client, err := provider.CallStream(context.Background(), uuid.New(), route)
	require.NoError(t, err)

	// wait for fake node to signal receipt (avoid hanging on client.Closed())
	select {
	case <-fnode.done:
	case <-time.After(1 * time.Second):
		require.FailNow(t, "timeout waiting for node to receive envelope")
	}

	// Assert
	require.NotNil(t, fnode.received)
	require.IsType(t, &api.GetSpaces{}, fnode.received)

	// close client to let muxer cleanup if needed
	client.Close(nil)
}

func TestShouldReturnErrorWhenNodeManagerFailsDuringCallStream(t *testing.T) {
	expectedErr := errors.New("node create failed")
	provider := NewInProcBidiStreamProvider(context.Background(), &fakeNodeManager{err: expectedErr})

	client, err := provider.CallStream(context.Background(), uuid.New(), &api.GetSpaces{})

	require.ErrorIs(t, err, expectedErr)
	assert.Nil(t, client)
}

func TestShouldAllowNilContextWhenCallStreamInvoked(t *testing.T) {
	fnode := &fakeNode{done: make(chan struct{})}
	provider := NewInProcBidiStreamProvider(context.Background(), &fakeNodeManager{node: fnode})

	client, err := provider.CallStream(nilContextForTest(), uuid.New(), &api.GetSpaces{})
	require.NoError(t, err)

	select {
	case <-fnode.done:
	case <-time.After(1 * time.Second):
		require.FailNow(t, "timeout waiting for node to receive envelope")
	}

	require.NotNil(t, fnode.received)
	client.Close(nil)
}

func TestShouldTreatReconnectListenerMethodsAsNoOps(t *testing.T) {
	provider := NewInProcBidiStreamProvider(context.Background(), &fakeNodeManager{node: &fakeNode{}})
	listener := &fakeReconnectListener{}

	assert.NotPanics(t, func() {
		provider.RegisterReconnectListener(listener)
		provider.UnregisterReconnectListener(listener)
		provider.RegisterReconnectListener(nil)
		provider.UnregisterReconnectListener(nil)
	})
	assert.Equal(t, 0, listener.calls)
}
