package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
)

// SubscribeWorkersOption configures SubscribeWorkers.
type SubscribeWorkersOption func(*subscribeWorkersConfig)

type subscribeWorkersConfig struct {
	workerID                 uuid.UUID
	metadata                 map[string]string
	heartbeatIntervalSeconds int64
}

// WithWorkerID registers this process in the store worker map for the life of
// the subscription stream.
func WithWorkerID(workerID uuid.UUID) SubscribeWorkersOption {
	return func(cfg *subscribeWorkersConfig) {
		cfg.workerID = workerID
	}
}

// WithWorkerMetadata attaches opaque metadata to worker presence events.
func WithWorkerMetadata(metadata map[string]string) SubscribeWorkersOption {
	return func(cfg *subscribeWorkersConfig) {
		if len(metadata) == 0 {
			return
		}
		cfg.metadata = make(map[string]string, len(metadata))
		for key, value := range metadata {
			cfg.metadata[key] = value
		}
	}
}

// WithWorkerHeartbeatInterval sets the worker presence renewal period and
// server→client stream heartbeats. Stale workers are evicted after roughly 3×
// this interval (minimum 30s). Observers should not use this option.
func WithWorkerHeartbeatInterval(interval time.Duration) SubscribeWorkersOption {
	return func(cfg *subscribeWorkersConfig) {
		if interval <= 0 {
			cfg.heartbeatIntervalSeconds = 0
			return
		}
		seconds := int64(interval / time.Second)
		if interval%time.Second != 0 {
			seconds++
		}
		cfg.heartbeatIntervalSeconds = api.ClampWorkerPresenceHeartbeatIntervalSeconds(seconds)
	}
}

func (c *client) SubscribeWorkers(ctx context.Context, storeID uuid.UUID, handler func(*WorkerInventory), opts ...SubscribeWorkersOption) (api.Subscription, error) {
	cfg := subscribeWorkersConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	args := &api.SubscribeWorkers{
		WorkerID: cfg.workerID,
		Metadata: cfg.metadata,
	}
	if cfg.workerID != uuid.Nil {
		args.HeartbeatIntervalSeconds = api.WorkerPresenceRenewIntervalSeconds(cfg.heartbeatIntervalSeconds)
	}
	return c.subscribeWorkerInventory(ctx, storeID, args, handler)
}

type activeWorkerInventorySubscription struct {
	id               string
	storeID          uuid.UUID
	initMsg          api.Routeable
	handler          func(*WorkerInventory)
	cancel           context.CancelFunc
	stopped          chan struct{}
	done             <-chan struct{}
	ctx              context.Context
	failureCount     atomic.Int32
	status           atomic.Value
	lastError        atomic.Value
	handlerTimeouts  atomic.Int32
	handlerPanics    atomic.Int32
	coalescedUpdates atomic.Int64
	inFlightHandlers atomic.Int32
	mailboxCh        chan struct{}
	pendingMu        sync.Mutex
	pendingSet       bool
	pendingInventory WorkerInventory
}

type inventoryDecodeResult struct {
	inventory *WorkerInventory
	heartbeat bool
	err       error
}

func startInventoryReader(ctx context.Context, stream api.BidiStream) (<-chan inventoryDecodeResult, <-chan struct{}) {
	results := make(chan inventoryDecodeResult)
	done := make(chan struct{})

	go func() {
		defer close(done)
		defer close(results)

		for {
			envelope := &polymorphic.Envelope{}
			err := stream.Decode(envelope)

			result := inventoryDecodeResult{err: err}
			if err == nil && envelope.Content != nil {
				switch content := envelope.Content.(type) {
				case *api.WorkerInventory:
					inv := *content
					result.inventory = &inv
				case *api.Presence:
					result.heartbeat = content.Heartbeat
				default:
					err = fmt.Errorf("unexpected worker stream message type: %T", envelope.Content)
					result.err = err
				}
			}

			select {
			case results <- result:
			case <-ctx.Done():
				return
			}

			if err != nil {
				return
			}
		}
	}()

	return results, done
}

func stopInventoryReader(cancel context.CancelFunc, stream api.BidiStream, err error, done <-chan struct{}) {
	cancel()
	stream.Close(err)
	<-done
}

func effectiveInventoryHeartbeatTimeout(msg api.Routeable) time.Duration {
	args, ok := msg.(*api.SubscribeWorkers)
	if !ok || args == nil || args.WorkerID == uuid.Nil || args.HeartbeatIntervalSeconds <= 0 {
		return 0
	}
	if subscriptionHeartbeatTimeout > 0 {
		return subscriptionHeartbeatTimeout
	}
	return 3 * time.Duration(args.HeartbeatIntervalSeconds) * time.Second
}

func (c *client) registerWorkerInventorySubscription(id string, sub *activeWorkerInventorySubscription) {
	shard := c.subscriptionShard(id)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.workerInventorySubscriptions[id] = sub
}

func (c *client) unregisterWorkerInventorySubscription(id string) {
	shard := c.subscriptionShard(id)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	delete(shard.workerInventorySubscriptions, id)
}

func (s *activeWorkerInventorySubscription) setLastError(err error) {
	s.lastError.Store(err)
}

func (s *activeWorkerInventorySubscription) tryAcquireHandlerSlot(limit int) bool {
	v := normalizeHandlerConcurrency(limit)
	maxInFlight, ok := intToInt32InRange(v)
	if !ok {
		maxInFlight = int32(^uint32(0) >> 1)
	}
	for {
		current := s.inFlightHandlers.Load()
		if current >= maxInFlight {
			return false
		}
		if s.inFlightHandlers.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func (s *activeWorkerInventorySubscription) releaseHandlerSlot() {
	for {
		current := s.inFlightHandlers.Load()
		if current <= 0 {
			return
		}
		if s.inFlightHandlers.CompareAndSwap(current, current-1) {
			return
		}
	}
}

func (s *activeWorkerInventorySubscription) queuePendingInventory(inventory WorkerInventory) bool {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	coalesced := s.pendingSet
	if coalesced {
		s.coalescedUpdates.Add(1)
	}
	s.pendingInventory = inventory
	s.pendingSet = true
	return coalesced
}

func (s *activeWorkerInventorySubscription) loadLastError() error {
	if v := s.lastError.Load(); v != nil {
		if err, ok := v.(error); ok {
			return err
		}
	}
	return nil
}

func (s *activeWorkerInventorySubscription) pendingInventoryCount() int {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if s.pendingSet {
		return 1
	}
	return 0
}

func (s *activeWorkerInventorySubscription) takePendingInventory() (WorkerInventory, bool) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if !s.pendingSet {
		return WorkerInventory{}, false
	}
	inventory := s.pendingInventory
	s.pendingSet = false
	s.pendingInventory = WorkerInventory{}
	return inventory, true
}

func (s *activeWorkerInventorySubscription) restorePendingInventory(inventory WorkerInventory) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	s.pendingInventory = inventory
	s.pendingSet = true
}

func (c *client) signalWorkerInventoryMailbox(sub *activeWorkerInventorySubscription) {
	select {
	case sub.mailboxCh <- struct{}{}:
	default:
	}
}

func (c *client) enqueueWorkerInventory(sub *activeWorkerInventorySubscription, inventory WorkerInventory) {
	sub.queuePendingInventory(inventory)
	c.signalWorkerInventoryMailbox(sub)
}

func (c *client) runInventoryHandlerSafely(ctx context.Context, sub *activeWorkerInventorySubscription, handler func(*WorkerInventory), inventory *WorkerInventory) {
	defer func() {
		if r := recover(); r != nil {
			sub.handlerPanics.Add(1)
			slog.ErrorContext(ctx, "worker inventory handler panic",
				slog.String("subscription_id", sub.id),
				slog.String("store_id", sub.storeID.String()),
				slog.Any("panic", r))
		}
	}()
	handler(inventory)
}

func (c *client) runInventoryHandler(ctx context.Context, sub *activeWorkerInventorySubscription, handler func(*WorkerInventory), inventory *WorkerInventory) {
	if c.handlerTimeout <= 0 {
		c.runInventoryHandlerSafely(ctx, sub, handler, inventory)
		return
	}

	handlerCtx, cancel := context.WithTimeout(ctx, c.handlerTimeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.runInventoryHandlerSafely(ctx, sub, handler, inventory)
	}()

	select {
	case <-done:
	case <-handlerCtx.Done():
		if errors.Is(handlerCtx.Err(), context.DeadlineExceeded) {
			sub.handlerTimeouts.Add(1)
			slog.WarnContext(ctx, "worker inventory handler exceeded timeout",
				slog.String("subscription_id", sub.id),
				slog.Duration("timeout", c.handlerTimeout))
		}
	}
}

func (c *client) tryStartInventoryHandler(ctx context.Context, sub *activeWorkerInventorySubscription, handler func(*WorkerInventory), inventory WorkerInventory, wg *sync.WaitGroup) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}

	if !sub.tryAcquireHandlerSlot(c.maxConcurrentHandlers) {
		return false
	}

	wg.Add(1)
	go func(inv WorkerInventory) {
		defer wg.Done()
		defer sub.releaseHandlerSlot()
		c.runInventoryHandler(ctx, sub, handler, &inv)
		c.signalWorkerInventoryMailbox(sub)
	}(inventory)

	return true
}

func (c *client) drainPendingWorkerInventory(ctx context.Context, sub *activeWorkerInventorySubscription, handler func(*WorkerInventory), wg *sync.WaitGroup) {
	for {
		if ctx.Err() != nil {
			return
		}

		inventory, ok := sub.takePendingInventory()
		if !ok {
			return
		}

		if c.tryStartInventoryHandler(ctx, sub, handler, inventory, wg) {
			continue
		}

		sub.restorePendingInventory(inventory)
		return
	}
}

func (c *client) subscribeWorkerInventory(ctx context.Context, storeID uuid.UUID, initMsg api.Routeable, handler func(*WorkerInventory)) (api.Subscription, error) {
	subCtx, cancel := newDetachedCancelableContext(ctx)

	var stream api.BidiStream
	err := RetryWithBackoff(subCtx, c.policy, func(retryCtx context.Context) error {
		var err error
		stream, err = c.provider.CallStream(retryCtx, storeID, initMsg)
		return err
	})
	if err != nil {
		cancel()
		return nil, err
	}

	done := make(chan struct{})
	subID := uuid.New().String()

	activeSub := &activeWorkerInventorySubscription{
		id:        subID,
		storeID:   storeID,
		initMsg:   initMsg,
		handler:   handler,
		cancel:    cancel,
		stopped:   make(chan struct{}),
		done:      done,
		ctx:       subCtx,
		mailboxCh: make(chan struct{}, 1),
	}
	activeSub.status.Store("active")
	c.registerWorkerInventorySubscription(subID, activeSub)

	go func(stream api.BidiStream) {
		defer close(done)
		defer close(activeSub.stopped)
		defer c.unregisterWorkerInventorySubscription(subID)

		var handlerWg sync.WaitGroup
		defer handlerWg.Wait()

		if args, ok := activeSub.initMsg.(*api.SubscribeWorkers); ok && args.WorkerID != uuid.Nil {
			go c.streamWorkerPresenceRenewals(subCtx, stream, args.WorkerID, args.HeartbeatIntervalSeconds)
		}

		heartbeatTimeout := effectiveInventoryHeartbeatTimeout(activeSub.initMsg)
		lastFrameAt := time.Now()
		streamReadCtx, cancelStreamRead := context.WithCancel(subCtx)
		decodeCh, readerDone := startInventoryReader(streamReadCtx, stream)

		for {
			var (
				timer     *time.Timer
				timeoutCh <-chan time.Time
			)
			if heartbeatTimeout > 0 {
				wait := time.Until(lastFrameAt.Add(heartbeatTimeout))
				if wait < 0 {
					wait = 0
				}
				timer = time.NewTimer(wait)
				timeoutCh = timer.C
			}

			select {
			case <-subCtx.Done():
				stopTimer(timer)
				stopInventoryReader(cancelStreamRead, stream, subCtx.Err(), readerDone)
				return
			case <-activeSub.mailboxCh:
				stopTimer(timer)
				c.drainPendingWorkerInventory(subCtx, activeSub, handler, &handlerWg)
				continue
			case <-timeoutCh:
				stopTimer(timer)
				timeoutErr := fmt.Errorf("%w after %s", errSubscriptionHeartbeatLost, heartbeatTimeout)
				activeSub.failureCount.Add(1)
				activeSub.status.Store("failed")
				activeSub.setLastError(timeoutErr)
				stopInventoryReader(cancelStreamRead, stream, timeoutErr, readerDone)
				return
			case result, ok := <-decodeCh:
				stopTimer(timer)
				if !ok {
					streamErr := stream.EndOfStreamError()
					if streamErr == nil {
						streamErr = errSubscriptionReaderStopped
					}
					activeSub.failureCount.Add(1)
					activeSub.status.Store("failed")
					activeSub.setLastError(streamErr)
					stopInventoryReader(cancelStreamRead, stream, streamErr, readerDone)
					return
				}
				if result.err != nil {
					activeSub.failureCount.Add(1)
					activeSub.status.Store("failed")
					activeSub.setLastError(result.err)
					stopInventoryReader(cancelStreamRead, stream, result.err, readerDone)
					return
				}

				lastFrameAt = time.Now()
				if result.heartbeat || result.inventory == nil {
					continue
				}

				inventory := *result.inventory
				if !c.tryStartInventoryHandler(subCtx, activeSub, handler, inventory, &handlerWg) {
					c.enqueueWorkerInventory(activeSub, inventory)
				}
				c.drainPendingWorkerInventory(subCtx, activeSub, handler, &handlerWg)
			}
		}
	}(stream)

	return &subscription{id: subID, cancel: cancel, done: done}, nil
}

func (c *client) streamWorkerPresenceRenewals(ctx context.Context, stream api.BidiStream, workerID uuid.UUID, renewIntervalSeconds int64) {
	if workerID == uuid.Nil || stream == nil {
		return
	}

	interval := time.Duration(api.WorkerPresenceRenewIntervalSeconds(renewIntervalSeconds)) * time.Second
	sendRenewal := func() error {
		return stream.Encode(polymorphic.NewEnvelope(&api.Presence{
			WorkerID:  workerID,
			Heartbeat: false,
			Timestamp: api.NowUnixMilli(),
		}))
	}

	if err := sendRenewal(); err != nil {
		stream.Close(err)
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := sendRenewal(); err != nil {
				stream.Close(err)
				return
			}
		}
	}
}
