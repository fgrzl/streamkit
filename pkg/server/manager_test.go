package server

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgrzl/streamkit/pkg/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testStoreFactory struct {
	created int
}

func (f *testStoreFactory) NewStore(ctx context.Context, storeID uuid.UUID) (storage.Store, error) {
	f.created++
	return &mockStore{}, nil
}

type scriptedStoreFactory struct {
	calls             int
	failuresRemaining int
	err               error
	store             storage.Store
}

func (f *scriptedStoreFactory) NewStore(ctx context.Context, storeID uuid.UUID) (storage.Store, error) {
	f.calls++
	if f.failuresRemaining > 0 {
		f.failuresRemaining--
		return nil, f.err
	}
	if f.store != nil {
		return f.store, nil
	}
	return &mockStore{}, nil
}

type closeCountingStore struct {
	mockStore
	closeCalls int32
}

func (s *closeCountingStore) Close() {
	atomic.AddInt32(&s.closeCalls, 1)
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

func TestShouldOpenCircuitAfterConsecutiveFailures(t *testing.T) {
	expectedErr := errors.New("store create failed")
	factory := &scriptedStoreFactory{failuresRemaining: 3, err: expectedErr}
	manager := NewNodeManager(WithStoreFactory(factory)).(*nodeManager)
	manager.failureWindow = time.Hour
	manager.maxFailures = 3
	storeID := uuid.New()

	for range 3 {
		_, err := manager.GetOrCreate(context.Background(), storeID)
		require.ErrorIs(t, err, expectedErr)
	}

	_, err := manager.GetOrCreate(context.Background(), storeID)

	require.Error(t, err)
	assert.ErrorContains(t, err, "circuit open")
	assert.Equal(t, 3, factory.calls)
}

func TestShouldRetryAfterFailureWindowExpires(t *testing.T) {
	expectedErr := errors.New("store create failed")
	factory := &scriptedStoreFactory{failuresRemaining: 3, err: expectedErr}
	manager := NewNodeManager(WithStoreFactory(factory)).(*nodeManager)
	manager.failureWindow = time.Minute
	manager.maxFailures = 3
	storeID := uuid.New()

	for range 3 {
		_, err := manager.GetOrCreate(context.Background(), storeID)
		require.ErrorIs(t, err, expectedErr)
	}

	manager.failures[storeID].lastFailed = time.Now().Add(-manager.failureWindow - time.Second)

	node, err := manager.GetOrCreate(context.Background(), storeID)

	require.NoError(t, err)
	require.NotNil(t, node)
	assert.Equal(t, 4, factory.calls)
	_, exists := manager.failures[storeID]
	assert.False(t, exists)
}

func TestShouldClearFailureStateAfterSuccessfulRecovery(t *testing.T) {
	expectedErr := errors.New("store create failed")
	factory := &scriptedStoreFactory{failuresRemaining: 1, err: expectedErr}
	manager := NewNodeManager(WithStoreFactory(factory)).(*nodeManager)
	storeID := uuid.New()

	_, err := manager.GetOrCreate(context.Background(), storeID)
	require.ErrorIs(t, err, expectedErr)
	_, exists := manager.failures[storeID]
	require.True(t, exists)

	node, err := manager.GetOrCreate(context.Background(), storeID)

	require.NoError(t, err)
	require.NotNil(t, node)
	assert.Equal(t, 2, factory.calls)
	_, exists = manager.failures[storeID]
	assert.False(t, exists)
}

func TestShouldPruneExpiredFailureEntriesOnLookup(t *testing.T) {
	factory := &scriptedStoreFactory{}
	manager := NewNodeManager(WithStoreFactory(factory)).(*nodeManager)
	manager.failureWindow = time.Minute

	expiredStoreID := uuid.New()
	recentStoreID := uuid.New()
	manager.failures[expiredStoreID] = &storeFailure{count: 2, lastFailed: time.Now().Add(-manager.failureWindow - time.Second)}
	manager.failures[recentStoreID] = &storeFailure{count: 1, lastFailed: time.Now()}

	_, err := manager.GetOrCreate(context.Background(), uuid.New())
	require.NoError(t, err)

	_, expiredExists := manager.failures[expiredStoreID]
	_, recentExists := manager.failures[recentStoreID]
	assert.False(t, expiredExists)
	assert.True(t, recentExists)
}

func TestShouldCloseManagedNodesOnlyOnce(t *testing.T) {
	store := &closeCountingStore{}
	factory := &scriptedStoreFactory{store: store}
	manager := NewNodeManager(WithStoreFactory(factory)).(*nodeManager)
	storeID := uuid.New()

	_, err := manager.GetOrCreate(context.Background(), storeID)
	require.NoError(t, err)
	manager.failures[uuid.New()] = &storeFailure{count: 1, lastFailed: time.Now()}

	manager.Close()
	manager.Close()

	assert.Equal(t, int32(1), atomic.LoadInt32(&store.closeCalls))
	assert.Empty(t, manager.nodes)
	assert.Empty(t, manager.failures)
}
