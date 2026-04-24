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
	r1 := n1.(*refCountedNode)
	r2 := n2.(*refCountedNode)
	require.Equal(t, r1.entry, r2.entry, "expected same backing node entry for repeated GetOrCreate")

	// Act: remove and create again
	// Act: remove and create again
	m.Remove(context.Background(), id)
	n3, err := m.GetOrCreate(context.Background(), id)
	require.NoError(t, err)

	// Assert: store factory should have been used to create a new store instance
	require.Equal(t, 2, sf.created, "expected store factory to be invoked twice")
	r3 := n3.(*refCountedNode)
	require.NotEqual(t, r1.entry, r3.entry)
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

func TestShouldCapFailuresMapSize(t *testing.T) {
	expectedErr := errors.New("store create failed")
	factory := &scriptedStoreFactory{failuresRemaining: 1_000_000, err: expectedErr}
	manager := NewNodeManager(WithStoreFactory(factory)).(*nodeManager)
	manager.maxFailureEntries = 32

	for range 200 {
		_, err := manager.GetOrCreate(context.Background(), uuid.New())
		require.ErrorIs(t, err, expectedErr)
	}

	manager.mu.Lock()
	n := len(manager.failures)
	manager.mu.Unlock()
	assert.LessOrEqual(t, n, 32)
}

func TestShouldEvictOldestFailureEntryWhenCapped(t *testing.T) {
	expectedErr := errors.New("store create failed")
	factory := &scriptedStoreFactory{failuresRemaining: 1_000_000, err: expectedErr}
	manager := NewNodeManager(WithStoreFactory(factory)).(*nodeManager)
	manager.maxFailureEntries = 32
	ctx := context.Background()

	idA := uuid.New()
	_, err := manager.GetOrCreate(ctx, idA)
	require.ErrorIs(t, err, expectedErr)

	time.Sleep(5 * time.Millisecond)

	for range 31 {
		_, err := manager.GetOrCreate(ctx, uuid.New())
		require.ErrorIs(t, err, expectedErr)
	}

	manager.mu.Lock()
	_, hasA := manager.failures[idA]
	nBefore := len(manager.failures)
	manager.mu.Unlock()
	require.True(t, hasA)
	require.Equal(t, 32, nBefore)

	_, err = manager.GetOrCreate(ctx, uuid.New())
	require.ErrorIs(t, err, expectedErr)

	manager.mu.Lock()
	_, hasAAfter := manager.failures[idA]
	manager.mu.Unlock()
	assert.False(t, hasAAfter, "oldest failure idA should have been evicted when map was at cap")
}

func TestShouldEvictIdleNodesAfterTTL(t *testing.T) {
	store := &closeCountingStore{}
	factory := &scriptedStoreFactory{store: store}
	m := NewNodeManager(
		WithStoreFactory(factory),
		WithIdleEviction(20*time.Millisecond, 5*time.Millisecond),
	).(*nodeManager)
	id := uuid.New()
	_, err := m.GetOrCreate(context.Background(), id)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		m.mu.Lock()
		_, ok := m.nodes[id]
		m.mu.Unlock()
		return !ok && atomic.LoadInt32(&store.closeCalls) >= 1
	}, time.Second, 5*time.Millisecond)
	m.Close()
}

func TestShouldNotEvictNodeWithInFlightHandle(t *testing.T) {
	store := &closeCountingStore{}
	factory := &scriptedStoreFactory{store: store}
	m := NewNodeManager(
		WithStoreFactory(factory),
		WithIdleEviction(25*time.Millisecond, 5*time.Millisecond),
	).(*nodeManager)
	id := uuid.New()
	node, err := m.GetOrCreate(context.Background(), id)
	require.NoError(t, err)
	ent := m.nodes[id]
	ent.inflight.Store(1)
	time.Sleep(80 * time.Millisecond)
	m.mu.Lock()
	_, still := m.nodes[id]
	m.mu.Unlock()
	require.True(t, still)
	ent.inflight.Store(0)
	// reapIdleNodes deletes under m.mu then closes without holding the lock; wait
	// until both eviction and store Close are visible to avoid a scheduler race.
	require.Eventually(t, func() bool {
		m.mu.Lock()
		_, ok := m.nodes[id]
		m.mu.Unlock()
		return !ok && atomic.LoadInt32(&store.closeCalls) >= 1
	}, time.Second, 5*time.Millisecond)
	_ = node
	m.Close()
}

func TestShouldRefreshLastAccessedOnGetOrCreate(t *testing.T) {
	store := &closeCountingStore{}
	factory := &scriptedStoreFactory{store: store}
	m := NewNodeManager(
		WithStoreFactory(factory),
		WithIdleEviction(50*time.Millisecond, 10*time.Millisecond),
	).(*nodeManager)
	id := uuid.New()
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		_, err := m.GetOrCreate(context.Background(), id)
		require.NoError(t, err)
		time.Sleep(15 * time.Millisecond)
	}
	m.mu.Lock()
	_, still := m.nodes[id]
	m.mu.Unlock()
	require.True(t, still, "node should remain while repeatedly refreshed")
	m.Close()
}

func TestShouldNotBreakWhenIdleEvictionDisabled(t *testing.T) {
	sf := &testStoreFactory{}
	m := NewNodeManager(WithStoreFactory(sf))
	id := uuid.New()
	_, err := m.GetOrCreate(context.Background(), id)
	require.NoError(t, err)
	m.Close()
	m.Close()
}
