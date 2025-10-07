package inprockit

import (
	"context"
	"testing"
	"time"

	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/node"
	"github.com/google/uuid"
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
	node node.Node
}

func (m *fakeNodeManager) GetOrCreate(ctx context.Context, storeID uuid.UUID) (node.Node, error) {
	return m.node, nil
}

func (m *fakeNodeManager) Remove(ctx context.Context, storeID uuid.UUID) {}
func (m *fakeNodeManager) Close()                                        {}

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
