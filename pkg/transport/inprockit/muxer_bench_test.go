package inprockit

import (
	"context"
	"testing"

	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/server"
	"github.com/google/uuid"
)

// mockNode closes the incoming bidi stream immediately to avoid blocking.
type mockNode struct{}

func (n *mockNode) Handle(ctx context.Context, bidi api.BidiStream) {
	// Close immediately to let the muxer clean up without waiting for messages.
	bidi.Close(nil)
}

func (n *mockNode) Close() {}

// mockNodeManager returns a mockNode for every GetOrCreate call.
type mockNodeManager struct{}

func (m *mockNodeManager) GetOrCreate(ctx context.Context, storeID uuid.UUID) (server.Node, error) {
	return &mockNode{}, nil
}

func (m *mockNodeManager) Remove(ctx context.Context, storeID uuid.UUID) {}
func (m *mockNodeManager) Close()                                        {}

// BenchmarkMuxerRegister measures the cost of registering a stream on the muxer
// using a mock NodeManager that immediately closes server streams.
func BenchmarkMuxerRegister(b *testing.B) {
	nm := &mockNodeManager{}
	m := NewInProcMuxer(context.Background(), nm)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := m.Register(context.Background(), uuid.New())
		if err != nil {
			b.Fatalf("register failed: %v", err)
		}
	}
}
