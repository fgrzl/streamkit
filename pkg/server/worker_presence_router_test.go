package server

import (
	"context"
	"testing"
	"time"

	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streamkit/internal/lease"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var (
	testWorkerA = uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	testWorkerB = uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
)

func inventoryFromEncoded(msg any) (*api.WorkerInventory, bool) {
	switch v := msg.(type) {
	case *api.WorkerInventory:
		return v, true
	case *polymorphic.Envelope:
		inventory, ok := v.Content.(*api.WorkerInventory)
		return inventory, ok
	default:
		return nil, false
	}
}

func inventoryContains(inventory *api.WorkerInventory, workerID uuid.UUID) bool {
	if inventory == nil {
		return false
	}
	for _, worker := range inventory.Workers {
		if worker.WorkerID == workerID {
			return true
		}
	}
	return false
}

func TestShouldFanOutInventoryGivenSubscribeWithWorkerID(t *testing.T) {
	storeID := uuid.New()
	node := NewNode(storeID, &mockStore{}, &mockMessageBusFactory{bus: &mockMessageBus{}}, lease.NewStore())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	observerBidi := newMockBidi(&polymorphic.Envelope{Content: &api.SubscribeWorkers{}})
	observerBidi.encodedCh = make(chan any, 8)
	go node.Handle(ctx, observerBidi)

	workerBidi := newMockBidi(&polymorphic.Envelope{Content: &api.SubscribeWorkers{
		WorkerID: testWorkerA,
		Metadata: map[string]string{"role": "processor"},
	}})
	go node.Handle(ctx, workerBidi)

	require.Eventually(t, func() bool {
		select {
		case msg := <-observerBidi.encodedCh:
			inventory, ok := inventoryFromEncoded(msg)
			return ok && inventoryContains(inventory, testWorkerA)
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, workerBidi.CloseSend(nil))
	waitForClosed(t, workerBidi)

	require.Eventually(t, func() bool {
		select {
		case msg := <-observerBidi.encodedCh:
			inventory, ok := inventoryFromEncoded(msg)
			return ok && !inventoryContains(inventory, testWorkerA)
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, observerBidi.CloseSend(nil))
	waitForClosed(t, observerBidi)
}

func TestShouldIsolateInventoryRoutesByStoreID(t *testing.T) {
	storeA := uuid.New()
	storeB := uuid.New()

	nodeA := NewNode(storeA, &mockStore{}, &mockMessageBusFactory{bus: &mockMessageBus{}}, lease.NewStore())
	nodeB := NewNode(storeB, &mockStore{}, &mockMessageBusFactory{bus: &mockMessageBus{}}, lease.NewStore())

	ctx := context.Background()

	subA := newMockBidi(&polymorphic.Envelope{Content: &api.SubscribeWorkers{}})
	subA.encodedCh = make(chan any, 4)
	go nodeA.Handle(ctx, subA)

	subB := newMockBidi(&polymorphic.Envelope{Content: &api.SubscribeWorkers{}})
	subB.encodedCh = make(chan any, 4)
	go nodeB.Handle(ctx, subB)

	workerA := newMockBidi(&polymorphic.Envelope{Content: &api.SubscribeWorkers{WorkerID: testWorkerA}})
	go nodeA.Handle(ctx, workerA)

	require.Eventually(t, func() bool {
		select {
		case msg := <-subA.encodedCh:
			inventory, ok := inventoryFromEncoded(msg)
			return ok && inventoryContains(inventory, testWorkerA)
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	select {
	case msg := <-subB.encodedCh:
		if inventory, ok := inventoryFromEncoded(msg); ok && inventoryContains(inventory, testWorkerA) {
			t.Fatalf("store B subscriber should not receive store A worker, got %#v", inventory)
		}
	default:
	}
}

func TestShouldCoalesceInventoryUpdatesForSlowSubscriber(t *testing.T) {
	router := &workerPresenceRouter{
		node:        &defaultNode{storeID: uuid.New()},
		workers:     make(map[uuid.UUID]*workerEntry),
		subscribers: make(map[string]*inventorySubscriberTarget),
	}
	target := newInventorySubscriberTarget(router, &slowEncodeBidi{})
	target.start()

	inv1 := &api.WorkerInventory{Workers: []api.WorkerInfo{{WorkerID: testWorkerA}}}
	inv2 := &api.WorkerInventory{Workers: []api.WorkerInfo{{WorkerID: testWorkerA}, {WorkerID: testWorkerB}}}
	target.acceptInventory(inv1)
	target.acceptInventory(inv2)

	target.mu.Lock()
	require.NotNil(t, target.sendQueue)
	require.Len(t, target.sendQueue.Workers, 2)
	target.mu.Unlock()
}

func TestShouldDeliverInventorySnapshotBeforeLiveUpdates(t *testing.T) {
	storeID := uuid.New()
	node := NewNode(storeID, &mockStore{}, &mockMessageBusFactory{bus: &mockMessageBus{}}, lease.NewStore())
	ctx := context.Background()

	workerA := newMockBidi(&polymorphic.Envelope{Content: &api.SubscribeWorkers{WorkerID: testWorkerA}})
	go node.Handle(ctx, workerA)

	subBidi := newMockBidi(&polymorphic.Envelope{Content: &api.SubscribeWorkers{}})
	subBidi.encodedCh = make(chan any, 8)
	go node.Handle(ctx, subBidi)

	require.Eventually(t, func() bool {
		select {
		case msg := <-subBidi.encodedCh:
			inventory, ok := inventoryFromEncoded(msg)
			return ok && inventoryContains(inventory, testWorkerA)
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	workerB := newMockBidi(&polymorphic.Envelope{Content: &api.SubscribeWorkers{WorkerID: testWorkerB}})
	go node.Handle(ctx, workerB)

	require.Eventually(t, func() bool {
		select {
		case msg := <-subBidi.encodedCh:
			inventory, ok := inventoryFromEncoded(msg)
			return ok && inventoryContains(inventory, testWorkerB)
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

type slowEncodeBidi struct {
	closed chan struct{}
}

func (s *slowEncodeBidi) Encode(any) error {
	time.Sleep(10 * time.Millisecond)
	return nil
}

func (s *slowEncodeBidi) Decode(any) error { return nil }
func (s *slowEncodeBidi) CloseSend(error) error {
	if s.closed == nil {
		s.closed = make(chan struct{})
	}
	close(s.closed)
	return nil
}
func (s *slowEncodeBidi) Close(error) {
	if s.closed == nil {
		s.closed = make(chan struct{})
	}
	close(s.closed)
}
func (s *slowEncodeBidi) EndOfStreamError() error { return nil }
func (s *slowEncodeBidi) Closed() <-chan struct{} {
	if s.closed == nil {
		s.closed = make(chan struct{})
	}
	return s.closed
}

func TestRenewWorkerUpdatesLastSeenWithoutFanOut(t *testing.T) {
	router := &workerPresenceRouter{
		node:        &defaultNode{storeID: uuid.New()},
		workers:     make(map[uuid.UUID]*workerEntry),
		workerIDs:   make([]uuid.UUID, 0),
		subscribers: make(map[string]*inventorySubscriberTarget),
	}
	subscriber := newInventorySubscriberTarget(router, newNopEncodeBidi())
	require.NoError(t, router.registerWorker(testWorkerA, nil, subscriber, 30))

	before := router.workers[testWorkerA].lastSeenAt
	time.Sleep(2 * time.Millisecond)
	router.renewWorker(testWorkerA, subscriber.id, api.NowUnixMilli())
	after := router.workers[testWorkerA].lastSeenAt
	require.Greater(t, after, before)
}

func TestShouldEvictStaleWorker(t *testing.T) {
	router := &workerPresenceRouter{
		node:        &defaultNode{storeID: uuid.New()},
		workers:     make(map[uuid.UUID]*workerEntry),
		workerIDs:   make([]uuid.UUID, 0),
		subscribers: make(map[string]*inventorySubscriberTarget),
	}
	observerBidi := newNopEncodeBidi()
	observer := newInventorySubscriberTarget(router, observerBidi)
	router.subscribers[observer.id] = observer
	observer.start()

	workerBidi := newNopEncodeBidi()
	worker := newInventorySubscriberTarget(router, workerBidi)
	router.subscribers[worker.id] = worker
	require.NoError(t, router.registerWorker(testWorkerA, nil, worker, 30))

	router.mu.Lock()
	router.workers[testWorkerA].lastSeenAt = time.Now().Add(-2 * api.WorkerPresenceTTL(30)).UnixMilli()
	router.mu.Unlock()

	router.evictStaleWorkers()

	router.mu.RLock()
	_, stillPresent := router.workers[testWorkerA]
	router.mu.RUnlock()
	require.False(t, stillPresent)

	worker.mu.Lock()
	closed := worker.closed
	worker.mu.Unlock()
	require.True(t, closed)
}

func TestShouldReplaceWorkerWhenDuplicateWorkerID(t *testing.T) {
	router := &workerPresenceRouter{
		node:        &defaultNode{storeID: uuid.New()},
		workers:     make(map[uuid.UUID]*workerEntry),
		workerIDs:   make([]uuid.UUID, 0),
		subscribers: make(map[string]*inventorySubscriberTarget),
	}

	priorBidi := newMockBidi(nil)
	prior := newInventorySubscriberTarget(router, priorBidi)
	router.subscribers[prior.id] = prior
	require.NoError(t, router.registerWorker(testWorkerA, nil, prior, 30))

	replacementBidi := newNopEncodeBidi()
	replacement := newInventorySubscriberTarget(router, replacementBidi)
	router.subscribers[replacement.id] = replacement
	require.NoError(t, router.registerWorker(testWorkerA, nil, replacement, 30))

	require.Equal(t, 1, priorBidi.closeCall)
	require.Equal(t, 0, priorBidi.closeLocalCall)
	require.ErrorIs(t, priorBidi.closeErr, errWorkerIDRegisteredElsewhere)

	prior.mu.Lock()
	closed := prior.closed
	prior.mu.Unlock()
	require.True(t, closed)

	router.mu.RLock()
	entry := router.workers[testWorkerA]
	router.mu.RUnlock()
	require.NotNil(t, entry)
	require.Equal(t, replacement.id, entry.subscriberID)
}

func TestShouldRemoveWorkerOnMalformedRenewalFrame(t *testing.T) {
	storeID := uuid.New()
	node := NewNode(storeID, &mockStore{}, &mockMessageBusFactory{bus: &mockMessageBus{}}, lease.NewStore())
	ctx := context.Background()

	observerBidi := newMockBidi(&polymorphic.Envelope{Content: &api.SubscribeWorkers{}})
	observerBidi.encodedCh = make(chan any, 8)
	go node.Handle(ctx, observerBidi)

	workerBidi := newMockBidi(&polymorphic.Envelope{Content: &api.SubscribeWorkers{
		WorkerID:                 testWorkerA,
		HeartbeatIntervalSeconds: 30,
	}})
	workerBidi.decodeQueue = []*polymorphic.Envelope{
		{Content: &api.SegmentStatus{Space: "bad", Segment: "frame"}},
	}
	go node.Handle(ctx, workerBidi)

	require.Eventually(t, func() bool {
		select {
		case msg := <-observerBidi.encodedCh:
			inventory, ok := inventoryFromEncoded(msg)
			return ok && inventoryContains(inventory, testWorkerA)
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		select {
		case msg := <-observerBidi.encodedCh:
			inventory, ok := inventoryFromEncoded(msg)
			return ok && !inventoryContains(inventory, testWorkerA)
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	waitForClosed(t, workerBidi)
}
