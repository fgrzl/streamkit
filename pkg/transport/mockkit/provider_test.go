package mockkit

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

func TestMockProvider_CallStream_EnvelopeDelivered(t *testing.T) {
	t.Run("Given a mock provider with a fake node", func(t *testing.T) {
		fnode := &fakeNode{done: make(chan struct{})}
		nm := &fakeNodeManager{node: fnode}

		provider := NewMockBidiStreamProvider(context.Background(), nm)

		t.Run("When CallStream is invoked with a route", func(t *testing.T) {
			route := &api.GetSpaces{}
			client, err := provider.CallStream(context.Background(), uuid.New(), route)
			require.NoError(t, err)

			// wait for fake node to signal receipt (avoid hanging on client.Closed())
			select {
			case <-fnode.done:
			case <-time.After(1 * time.Second):
				require.FailNow(t, "timeout waiting for node to receive envelope")
			}

			t.Run("Then the node should receive the decoded envelope content", func(t *testing.T) {
				require.NotNil(t, fnode.received)
				require.IsType(t, &api.GetSpaces{}, fnode.received)
			})

			// close client to let muxer cleanup if needed
			client.Close(nil)
		})
	})
}
