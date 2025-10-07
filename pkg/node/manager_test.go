package node

import (
	"context"
	"testing"

	"github.com/fgrzl/streamkit/pkg/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type testStoreFactory struct {
	created int
}

func (f *testStoreFactory) NewStore(ctx context.Context, storeID uuid.UUID) (storage.Store, error) {
	f.created++
	return &mockStore{}, nil
}

func TestShouldGetOrCreateAndRemoveNode(t *testing.T) {
	// Arrange
	sf := &testStoreFactory{}
	m := NewNodeManager(WithStoreFactory(sf))

	id := uuid.New()

	// Act + Assert
	n1, err := m.GetOrCreate(context.Background(), id)
	require.NoError(t, err)

	n2, err := m.GetOrCreate(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, n1, n2, "expected same node instance for repeated GetOrCreate")

	// Act: remove and create again
	// Act: remove and create again
	m.Remove(context.Background(), id)
	n3, err := m.GetOrCreate(context.Background(), id)
	require.NoError(t, err)

	// Assert: store factory should have been used to create a new store instance
	require.Equal(t, 2, sf.created, "expected store factory to be invoked twice")
	_ = n3
}
