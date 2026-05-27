// Worker presence is node-local: one server process owns the worker map per store.
// Many clients may connect—workers register via SubscribeWorkers with WorkerID,
// observers receive full inventory snapshots and live fan-out on change.
package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/telemetry"
	"go.opentelemetry.io/otel/trace"
)

func (n *defaultNode) getOrCreateWorkerPresenceRouter() *workerPresenceRouter {
	n.workerPresenceRouterMu.Lock()
	defer n.workerPresenceRouterMu.Unlock()

	if n.workerPresenceRouter != nil {
		return n.workerPresenceRouter
	}

	n.workerPresenceRouter = &workerPresenceRouter{
		node:        n,
		workers:     make(map[uuid.UUID]*workerEntry),
		workerIDs:   make([]uuid.UUID, 0),
		subscribers: make(map[string]*inventorySubscriberTarget),
	}
	return n.workerPresenceRouter
}

type workerEntry struct {
	workerID                uuid.UUID
	metadata                map[string]string
	subscriberID            string
	lastSeenAt              int64
	presenceIntervalSeconds int64
}

type workerPresenceRouter struct {
	node            *defaultNode
	mu              sync.RWMutex
	workers         map[uuid.UUID]*workerEntry
	workerIDs       []uuid.UUID
	subscribers     map[string]*inventorySubscriberTarget
	subscriberCount int
	closeOnce       sync.Once
	sweeperOnce     sync.Once
	sweeperCtx      context.Context
	sweeperCancel   context.CancelFunc
}

func (r *workerPresenceRouter) registerSubscriber(bidi api.BidiStream) *inventorySubscriberTarget {
	target := newInventorySubscriberTarget(r, bidi)

	r.mu.Lock()
	r.subscribers[target.id] = target
	r.subscriberCount++
	r.mu.Unlock()

	target.start()
	return target
}

func (r *workerPresenceRouter) registerWorker(workerID uuid.UUID, metadata map[string]string, subscriber *inventorySubscriberTarget, presenceIntervalSeconds int64) error {
	if workerID == uuid.Nil || subscriber == nil {
		return fmt.Errorf("worker_id is required")
	}

	renewSeconds := workerPresenceRenewIntervalSeconds(presenceIntervalSeconds)
	now := api.NowUnixMilli()

	var priorTarget *inventorySubscriberTarget
	r.mu.Lock()
	if existing, ok := r.workers[workerID]; ok && existing.subscriberID != subscriber.id {
		priorTarget = r.subscribers[existing.subscriberID]
	}
	if _, ok := r.workers[workerID]; !ok {
		r.insertWorkerIDLocked(workerID)
	}
	r.workers[workerID] = &workerEntry{
		workerID:                workerID,
		metadata:                cloneWorkerMetadata(metadata),
		subscriberID:            subscriber.id,
		lastSeenAt:              now,
		presenceIntervalSeconds: renewSeconds,
	}
	subscriber.workerID = workerID
	inventory, targets := r.snapshotForFanOutLocked(now)
	r.mu.Unlock()

	if priorTarget != nil {
		slog.WarnContext(context.Background(), "server: replacing prior worker registration",
			logContextFields(context.Background(), r.node.storeID,
				slog.String("worker_id", workerID.String()))...)
		priorTarget.close(errWorkerIDRegisteredElsewhere, true, true)
	}

	r.ensurePresenceSweeper()
	r.deliverInventory(inventory, targets)
	return nil
}

func (r *workerPresenceRouter) insertWorkerIDLocked(workerID uuid.UUID) {
	i := sort.Search(len(r.workerIDs), func(i int) bool {
		return api.CompareWorkerID(workerID, r.workerIDs[i]) < 0
	})
	if i < len(r.workerIDs) && r.workerIDs[i] == workerID {
		return
	}
	r.workerIDs = append(r.workerIDs, uuid.Nil)
	copy(r.workerIDs[i+1:], r.workerIDs[i:])
	r.workerIDs[i] = workerID
}

func (r *workerPresenceRouter) buildInventoryLocked(now int64) *api.WorkerInventory {
	inventory := &api.WorkerInventory{
		Timestamp: now,
		Workers:   make([]api.WorkerInfo, len(r.workerIDs)),
	}
	n := 0
	for _, workerID := range r.workerIDs {
		worker := r.workers[workerID]
		if worker == nil {
			continue
		}
		inventory.Workers[n] = api.WorkerInfo{
			WorkerID:   worker.workerID,
			Metadata:   worker.metadata,
			LastSeenAt: worker.lastSeenAt,
		}
		n++
	}
	inventory.Workers = inventory.Workers[:n]
	return inventory
}

func (r *workerPresenceRouter) snapshotForFanOutLocked(now int64) (*api.WorkerInventory, []*inventorySubscriberTarget) {
	return r.buildInventoryLocked(now), r.subscriberTargetsLocked()
}

func (r *workerPresenceRouter) currentInventory() *api.WorkerInventory {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.buildInventoryLocked(api.NowUnixMilli())
}

func (r *workerPresenceRouter) subscriberTargetsLocked() []*inventorySubscriberTarget {
	targets := make([]*inventorySubscriberTarget, 0, len(r.subscribers))
	for _, target := range r.subscribers {
		targets = append(targets, target)
	}
	return targets
}

func (r *workerPresenceRouter) deliverInventory(inventory *api.WorkerInventory, targets []*inventorySubscriberTarget) {
	for _, target := range targets {
		target.acceptInventory(inventory)
	}
}

func (r *workerPresenceRouter) removeWorkersForSubscriber(subscriberID string) {
	r.mu.Lock()
	for i := 0; i < len(r.workerIDs); {
		workerID := r.workerIDs[i]
		entry := r.workers[workerID]
		if entry != nil && entry.subscriberID == subscriberID {
			delete(r.workers, workerID)
			r.workerIDs = append(r.workerIDs[:i], r.workerIDs[i+1:]...)
			continue
		}
		i++
	}
	r.mu.Unlock()
}

func (r *workerPresenceRouter) unregisterSubscriber(target *inventorySubscriberTarget) {
	if r == nil || target == nil || target.id == "" {
		return
	}

	r.mu.Lock()
	delete(r.subscribers, target.id)
	if r.subscriberCount > 0 {
		r.subscriberCount--
	}
	r.mu.Unlock()
}

func (r *workerPresenceRouter) unregisterSubscriberWithPublish(target *inventorySubscriberTarget) {
	if r == nil || target == nil || target.id == "" {
		return
	}

	r.removeWorkersForSubscriber(target.id)

	r.mu.Lock()
	_, exists := r.subscribers[target.id]
	if exists {
		delete(r.subscribers, target.id)
		if r.subscriberCount > 0 {
			r.subscriberCount--
		}
	}
	r.mu.Unlock()

	if exists {
		r.publishInventory()
	}
}

func (r *workerPresenceRouter) close() {
	r.closeOnce.Do(func() {
		if r.sweeperCancel != nil {
			r.sweeperCancel()
		}

		r.mu.Lock()
		subscribers := make([]*inventorySubscriberTarget, 0, len(r.subscribers))
		for _, subscriber := range r.subscribers {
			subscribers = append(subscribers, subscriber)
		}
		r.workers = make(map[uuid.UUID]*workerEntry)
		r.workerIDs = r.workerIDs[:0]
		r.subscribers = make(map[string]*inventorySubscriberTarget)
		r.subscriberCount = 0
		r.mu.Unlock()

		for _, subscriber := range subscribers {
			subscriber.close(nil, true)
		}
	})
}

func (r *workerPresenceRouter) publishInventory() {
	if r == nil || r.node == nil {
		return
	}

	now := api.NowUnixMilli()
	r.mu.Lock()
	inventory, targets := r.snapshotForFanOutLocked(now)
	r.mu.Unlock()

	r.deliverInventory(inventory, targets)
}

func (r *workerPresenceRouter) dropSubscriber(ctx context.Context, target *inventorySubscriberTarget, err error) {
	if target == nil {
		return
	}
	slog.WarnContext(ctx, "server: failed to deliver worker inventory",
		logContextFields(ctx, r.node.storeID, "err", err)...)
	target.close(err, true)
}

type inventorySubscriberTarget struct {
	id        string
	workerID  uuid.UUID
	router    *workerPresenceRouter
	bidi      api.BidiStream
	mu        sync.Mutex
	encodeMu  sync.Mutex
	cond      *sync.Cond
	closeOnce sync.Once
	closed    bool
	sendQueue *api.WorkerInventory
}

func newInventorySubscriberTarget(router *workerPresenceRouter, bidi api.BidiStream) *inventorySubscriberTarget {
	target := &inventorySubscriberTarget{
		id:     uuid.NewString(),
		router: router,
		bidi:   bidi,
	}
	target.cond = sync.NewCond(&target.mu)
	return target
}

func (t *inventorySubscriberTarget) acceptInventory(inventory *api.WorkerInventory) {
	if inventory == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	t.enqueueInventoryLocked(inventory)
}

func (t *inventorySubscriberTarget) emitInventory(inventory *api.WorkerInventory) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.enqueueInventoryLocked(inventory)
	return nil
}

func (t *inventorySubscriberTarget) encodeWire(content polymorphic.Polymorphic) error {
	t.encodeMu.Lock()
	defer t.encodeMu.Unlock()

	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return io.ErrClosedPipe
	}

	return t.bidi.Encode(polymorphic.NewEnvelope(content))
}

func (t *inventorySubscriberTarget) enqueueInventoryLocked(inventory *api.WorkerInventory) {
	if inventory == nil {
		return
	}
	// Inventory snapshots are immutable after build; coalescing replaces the pointer.
	t.sendQueue = inventory
	if t.cond != nil {
		t.cond.Signal()
	}
}

func (t *inventorySubscriberTarget) start() {
	go t.run()
}

func (t *inventorySubscriberTarget) run() {
	for {
		t.mu.Lock()
		for t.sendQueue == nil && !t.closed {
			t.cond.Wait()
		}
		if t.closed {
			t.mu.Unlock()
			return
		}
		inventory := t.sendQueue
		t.sendQueue = nil
		t.mu.Unlock()

		if err := t.encodeWire(inventory); err != nil {
			if t.router != nil {
				t.router.dropSubscriber(context.Background(), t, err)
			} else {
				t.close(err, true)
			}
			return
		}
	}
}

func (t *inventorySubscriberTarget) close(err error, closeBidi bool, notifyRemote ...bool) {
	t.closeOnce.Do(func() {
		t.mu.Lock()
		t.closed = true
		t.sendQueue = nil
		if t.cond != nil {
			t.cond.Broadcast()
		}
		t.mu.Unlock()

		if !closeBidi {
			return
		}
		remote := len(notifyRemote) > 0 && notifyRemote[0]
		if remote {
			t.bidi.Close(err)
			return
		}
		closeSubscriptionBidi(t.bidi, err)
	})
}

func cloneWorkerMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(metadata))
	for key, value := range metadata {
		cloned[key] = value
	}
	return cloned
}

func (n *defaultNode) handleSubscribeWorkers(ctx context.Context, args *api.SubscribeWorkers, bidi api.BidiStream) {
	span := trace.SpanFromContext(ctx)
	router := n.getOrCreateWorkerPresenceRouter()
	subscriber := router.registerSubscriber(bidi)

	subCtx, cancelSub := context.WithCancel(ctx)
	var cleanupOnce sync.Once
	cleanup := func(closeErr error, closeBidi bool) {
		cleanupOnce.Do(func() {
			router.unregisterSubscriberWithPublish(subscriber)
			cancelSub()
			subscriber.close(closeErr, closeBidi)
		})
	}

	heartbeatSeconds := clampSubscriptionHeartbeatIntervalSeconds(args.HeartbeatIntervalSeconds)

	if args.WorkerID != uuid.Nil {
		if err := router.registerWorker(args.WorkerID, args.Metadata, subscriber, heartbeatSeconds); err != nil {
			telemetry.RecordError(span, err)
			slog.WarnContext(ctx, "server: failed to register worker",
				logContextFields(ctx, n.storeID, slog.String("worker_id", args.WorkerID.String()), "err", err)...)
			cleanup(err, true)
			return
		}
		go n.readWorkerPresenceRenewals(subCtx, args.WorkerID, subscriber, bidi, func(err error) {
			cleanup(err, true)
		})
	} else if err := subscriber.emitInventory(router.currentInventory()); err != nil {
		telemetry.RecordError(span, err)
		slog.WarnContext(ctx, "server: failed to encode worker inventory snapshot",
			logContextFields(ctx, n.storeID, "err", err)...)
		cleanup(err, true)
		return
	}

	if args.WorkerID != uuid.Nil && heartbeatSeconds > 0 {
		go n.streamPresenceHeartbeats(subCtx, args.WorkerID, heartbeatSeconds, subscriber)
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(subCtx, "server: worker presence subscription cleanup goroutine panic",
					logContextFields(subCtx, n.storeID, slog.Any("panic", r))...)
			}
		}()
		select {
		case <-bidi.Closed():
			cleanup(nil, false)
		case <-subCtx.Done():
			cleanup(subCtx.Err(), true)
		}
	}()
}

func (n *defaultNode) streamPresenceHeartbeats(ctx context.Context, workerID uuid.UUID, heartbeatSeconds int64, subscriber *inventorySubscriberTarget) {
	if workerID == uuid.Nil || subscriber == nil {
		return
	}
	interval := time.Duration(clampSubscriptionHeartbeatIntervalSeconds(heartbeatSeconds)) * time.Second
	if interval <= 0 {
		return
	}

	sendHeartbeat := func() error {
		return subscriber.encodeWire(&api.Presence{
			WorkerID:  workerID,
			Heartbeat: true,
			Timestamp: api.NowUnixMilli(),
		})
	}

	if err := sendHeartbeat(); err != nil {
		subscriber.close(err, true)
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-subscriber.bidi.Closed():
			return
		case <-ticker.C:
			if err := sendHeartbeat(); err != nil {
				subscriber.close(err, true)
				return
			}
		}
	}
}
