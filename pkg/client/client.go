// Package client provides a high-throughput, hierarchical event streaming client
// for interacting with streamkit servers.
//
// The package offers a client interface for interacting with streaming stores,
// spaces, and segments. A Store provides physical separation at the storage level,
// acting as the root for all spaces and segments. A Space is a top-level logical
// container for related streams, while Segments are independent, ordered sub-streams
// within a Space.
package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/telemetry"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

type (
	// Models
	Entry         = api.Entry
	Record        = api.Record
	SegmentStatus = api.SegmentStatus

	// Requests
	Consume        = api.Consume
	ConsumeSegment = api.ConsumeSegment
	ConsumeSpace   = api.ConsumeSpace
	GetSegments    = api.GetSegments
	GetSpaces      = api.GetSpaces
	GetStatus      = api.GetStatus
	Peek           = api.Peek
	Produce        = api.Produce

	// Interfaces
	Consumable = api.Consumable
	Routeable  = api.Routeable
)

var (
	// Application-level subscription heartbeats are opt-in. By default the
	// client relies on transport closure rather than synthetic keepalive frames.
	subscriptionHeartbeatInterval = time.Duration(0)
	subscriptionHeartbeatTimeout  = time.Duration(0)
	errSubscriptionHeartbeatLost  = errors.New("subscription heartbeat lost")
	errSubscriptionReaderStopped  = errors.New("subscription stream reader stopped unexpectedly")
	errInvalidConsumeEntry        = errors.New("invalid consume entry payload")
)

// ClientFactory defines how to create new Client instances.
type ClientFactory interface {
	Get(ctx context.Context) (Client, error)
}

// Client provides the main interface for interacting with the streaming platform.
// It supports operations for spaces, segments, consumption, production, and subscriptions.
type Client interface {
	// GetSpaces returns an enumerator of space names for the given store.
	GetSpaces(ctx context.Context, storeID uuid.UUID) enumerators.Enumerator[string]

	// GetSegments returns an enumerator of segment names within the specified space.
	GetSegments(ctx context.Context, storeID uuid.UUID, space string) enumerators.Enumerator[string]

	// Peek returns the latest entry in the specified segment without consuming it.
	Peek(ctx context.Context, storeID uuid.UUID, space, segment string) (*Entry, error)

	// Consume reads entries from multiple spaces based on the provided consumption arguments.
	Consume(ctx context.Context, storeID uuid.UUID, args *Consume) enumerators.Enumerator[*Entry]

	// ConsumeSpace reads entries from all segments within a space, returning them in timestamp order.
	ConsumeSpace(ctx context.Context, storeID uuid.UUID, args *ConsumeSpace) enumerators.Enumerator[*Entry]

	// ConsumeSegment reads entries from a specific segment in sequence order.
	ConsumeSegment(ctx context.Context, storeID uuid.UUID, args *ConsumeSegment) enumerators.Enumerator[*Entry]

	// Produce writes a stream of records to the specified segment and returns status updates.
	Produce(ctx context.Context, storeID uuid.UUID, space, segment string, entries enumerators.Enumerator[*Record]) enumerators.Enumerator[*SegmentStatus]

	// Publish writes a single record to the specified segment.
	Publish(ctx context.Context, storeID uuid.UUID, space, segment string, payload []byte, metadata map[string]string) error

	// SubscribeToSpace subscribes to status updates for all segments within a space.
	SubscribeToSpace(ctx context.Context, storeID uuid.UUID, space string, handler func(*SegmentStatus)) (api.Subscription, error)

	// SubscribeToSegment subscribes to status updates for a specific segment.
	SubscribeToSegment(ctx context.Context, storeID uuid.UUID, space, segment string, handler func(*SegmentStatus)) (api.Subscription, error)

	// GetSubscriptionStatus returns the current health status of a subscription (Issue 4 & 7).
	// Returns nil if subscription not found. Includes failure count, last sequence, handler timeouts/panics.
	GetSubscriptionStatus(id string) *SubscriptionStatus

	// WithLease acquires a lease for the key, runs fn with a context that is canceled when the lease
	// is released or lost (e.g. renew failed) or when ctx is canceled. The lease is released when fn
	// returns (or on panic). Best-effort: if the lease is lost, fn's context is canceled—fn should
	// respect that context (e.g. pass it to operations or select on it) so work stops when the lease is gone.
	WithLease(ctx context.Context, storeID uuid.UUID, key string, ttl time.Duration, fn func(context.Context) error) error

	// Close gracefully shuts down the client, stopping background goroutines and cleaning up resources.
	// Safe to call multiple times. Should be called when the client is no longer needed.
	Close() error
}

// NewClient creates a new Client instance using the provided BidiStreamProvider.
// The client retries transient failures while opening a new stream, but once a
// logical stream is established it is not resumed after a disconnect.
// Handler timeout is set to 30 seconds by default (Issue 7 - prevents slow handlers from blocking).
func NewClient(provider api.BidiStreamProvider) Client {
	return NewClientWithHandlerTimeout(provider, 30*time.Second)
}

// NewClientWithRetryPolicy creates a new Client with a custom retry policy.
func NewClientWithRetryPolicy(provider api.BidiStreamProvider, policy RetryPolicy) Client {
	c := &client{
		provider:              provider,
		policy:                policy,
		handlerTimeout:        30 * time.Second,
		maxConcurrentHandlers: 50,
		subscriptionShards:    newSubscriptionRegistryShards(),
		produceLocks:          make(map[string]*sync.Mutex),
	}
	c.startHandlerWorkers()
	return c
}

// NewClientWithHandlerTimeout creates a new Client with custom timeout for handler execution (Issue 7).
// Prevents slow handlers from blocking the subscription loop and starving other subscriptions.
// If a handler takes longer than the specified timeout, it is interrupted and execution continues
// with the next message. The timeout event is logged and tracked in subscription metrics.
// Set handlerTimeout <= 0 to disable timeout enforcement and run handlers inline.
func NewClientWithHandlerTimeout(provider api.BidiStreamProvider, handlerTimeout time.Duration) Client {
	return NewClientWithMetrics(provider, handlerTimeout, nil)
}

// NewClientWithMetrics creates a new Client with optional metrics. Pass nil for metrics to disable.
// Use NewOTelClientMetrics() for OpenTelemetry-backed produce/consume latency and
// subscription lifecycle/timeout/panic metrics. Set handlerTimeout <= 0 to disable
// timeout enforcement and run handlers inline.
func NewClientWithMetrics(provider api.BidiStreamProvider, handlerTimeout time.Duration, metrics ClientMetrics) Client {
	c := &client{
		provider:              provider,
		policy:                DefaultRetryPolicy(),
		handlerTimeout:        handlerTimeout,
		maxConcurrentHandlers: 50,
		subscriptionShards:    newSubscriptionRegistryShards(),
		produceLocks:          make(map[string]*sync.Mutex),
		metrics:               metrics,
	}
	c.startHandlerWorkers()

	return c
}

// activeSubscription tracks one live logical subscription and its delivery state.
type activeSubscription struct {
	id                  string                   // unique subscription ID
	storeID             uuid.UUID                // store being subscribed to
	initMsg             api.Routeable            // initial subscription message
	handler             func(*SegmentStatus)     // handler to call for each status update
	cancel              context.CancelFunc       // cancels the subscription goroutine
	stopped             chan struct{}            // signals when the subscription loop has stopped and unregistered
	done                <-chan struct{}          // signals when subscription has stopped
	lastDelivered       atomic.Int64             // tracks last delivered sequence for offset resumption
	ctx                 context.Context          // subscription's own context
	failureCount        atomic.Int32             // tracks consecutive failures for observability
	status              atomic.Value             // current status: "active"|"failed"
	lastError           atomic.Value             // stores last error encountered
	handlerTimeouts     atomic.Int32             // counts handler timeout occurrences
	handlerPanics       atomic.Int32             // counts handler panic occurrences
	coalescedUpdates    atomic.Int64             // counts updates merged while handlers are saturated
	inFlightHandlers    atomic.Int32             // current concurrent handler executions for the subscription
	mailboxCh           chan struct{}            // signals that a latest status is ready for delivery
	pendingMu           sync.Mutex               // guards pending delivery state
	pendingPerSegment   bool                     // wildcard subscriptions keep one latest status per segment
	pendingSingleSet    bool                     // whether the exact-segment pending slot is populated
	pendingSingleStatus SegmentStatus            // latest queued status for exact-segment subscriptions
	pendingStatusByKey  map[string]SegmentStatus // latest queued status per segment awaiting delivery
	pendingOrder        []string                 // FIFO order for pending segment keys
}

type subscriptionHandlerTask struct {
	ctx     context.Context
	sub     *activeSubscription
	subID   string
	handler func(*SegmentStatus)
	status  SegmentStatus
	wg      *sync.WaitGroup
}

const subscriptionRegistryShardCount = 32

type subscriptionRegistryShard struct {
	mu            sync.RWMutex
	subscriptions map[string]*activeSubscription
}

func newSubscriptionRegistryShards() []subscriptionRegistryShard {
	shards := make([]subscriptionRegistryShard, subscriptionRegistryShardCount)
	for i := range shards {
		shards[i].subscriptions = make(map[string]*activeSubscription)
	}
	return shards
}

func subscriptionShardIndex(id string, shardCount int) int {
	if shardCount <= 1 {
		return 0
	}

	var hash uint32 = 2166136261
	for i := 0; i < len(id); i++ {
		hash ^= uint32(id[i])
		hash *= 16777619
	}

	return int(hash % uint32(shardCount))
}

func (c *client) subscriptionShard(id string) *subscriptionRegistryShard {
	return &c.subscriptionShards[subscriptionShardIndex(id, len(c.subscriptionShards))]
}

func (c *client) getSubscription(id string) (*activeSubscription, bool) {
	shard := c.subscriptionShard(id)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	sub, exists := shard.subscriptions[id]
	return sub, exists
}

func (c *client) snapshotSubscriptions() []*activeSubscription {
	subs := make([]*activeSubscription, 0)
	for i := range c.subscriptionShards {
		shard := &c.subscriptionShards[i]
		shard.mu.RLock()
		for _, sub := range shard.subscriptions {
			subs = append(subs, sub)
		}
		shard.mu.RUnlock()
	}
	return subs
}

func (c *client) snapshotSubscriptionIDs() []string {
	ids := make([]string, 0)
	for i := range c.subscriptionShards {
		shard := &c.subscriptionShards[i]
		shard.mu.RLock()
		for id := range shard.subscriptions {
			ids = append(ids, id)
		}
		shard.mu.RUnlock()
	}
	return ids
}

func (c *client) subscriptionCount() int {
	count := 0
	for i := range c.subscriptionShards {
		shard := &c.subscriptionShards[i]
		shard.mu.RLock()
		count += len(shard.subscriptions)
		shard.mu.RUnlock()
	}
	return count
}

type storedSubscriptionError string

func (e storedSubscriptionError) Error() string {
	return string(e)
}

func (s *activeSubscription) setLastError(err error) {
	if s == nil || err == nil {
		return
	}
	s.lastError.Store(storedSubscriptionError(err.Error()))
}

func (s *activeSubscription) loadLastError() error {
	if s == nil {
		return nil
	}
	if e, ok := s.lastError.Load().(storedSubscriptionError); ok && e != "" {
		return e
	}
	return nil
}

// ClientMetrics provides instrumentation hooks for external observability systems.
// Implementations may be nil when metrics are not required.
type ClientMetrics interface {
	RecordProduceLatency(space, segment string, duration time.Duration)
	RecordConsumeLatency(space, segment string, duration time.Duration)
	RecordSubscriptionReplay(id string, success bool, duration time.Duration)
	RecordHandlerTimeout(id string)
	RecordHandlerPanic(id string)
}

type subscriptionOverloadMetrics interface {
	RecordSubscriptionCoalesced(id string)
}

type client struct {
	provider              api.BidiStreamProvider
	policy                RetryPolicy
	handlerTimeout        time.Duration // max time allowed for handler to execute (Issue 7)
	maxConcurrentHandlers int           // max concurrent handler executions tracked per subscription
	handlerWorkerCount    int           // number of shared workers processing subscription handlers
	handlerTaskQueue      chan subscriptionHandlerTask
	handlerWorkersCancel  context.CancelFunc
	handlerWorkersWg      sync.WaitGroup

	// subscription registry for tracking active subscriptions
	subscriptionShards []subscriptionRegistryShard
	shutdownOnce       sync.Once

	// Issue 9: Per-segment locks for atomic Peek+Produce
	produceLocksLock sync.RWMutex
	produceLocks     map[string]*sync.Mutex // keyed by {storeID}:{space}:{segment}

	// Optional metrics hooks for Prometheus/OpenTelemetry
	metrics ClientMetrics
}

func (c *client) GetSpaces(ctx context.Context, storeID uuid.UUID) enumerators.Enumerator[string] {
	var bidi api.BidiStream
	err := RetryWithBackoff(ctx, c.policy, func(retryCtx context.Context) error {
		var err error
		bidi, err = c.provider.CallStream(retryCtx, storeID, &api.GetSpaces{})
		return err
	})
	if err != nil {
		return enumerators.Error[string](err)
	}
	return api.NewStreamEnumerator[string](bidi)
}

func (c *client) GetSegments(ctx context.Context, storeID uuid.UUID, space string) enumerators.Enumerator[string] {
	var bidi api.BidiStream
	err := RetryWithBackoff(ctx, c.policy, func(retryCtx context.Context) error {
		var err error
		bidi, err = c.provider.CallStream(retryCtx, storeID, &api.GetSegments{Space: space})
		return err
	})
	if err != nil {
		return enumerators.Error[string](err)
	}
	return api.NewStreamEnumerator[string](bidi)
}

func (c *client) ConsumeSpace(ctx context.Context, storeID uuid.UUID, args *api.ConsumeSpace) enumerators.Enumerator[*Entry] {
	ctx = ensureRequestIDContext(ctx)
	return newConsumeEnumerator(c, ctx, storeID, args)
}

func (c *client) ConsumeSegment(ctx context.Context, storeID uuid.UUID, args *api.ConsumeSegment) enumerators.Enumerator[*Entry] {
	ctx = ensureRequestIDContext(ctx)
	return newConsumeEnumerator(c, ctx, storeID, args)
}

func (c *client) Consume(ctx context.Context, storeID uuid.UUID, args *api.Consume) enumerators.Enumerator[*Entry] {
	ctx = ensureRequestIDContext(ctx)
	return newConsumeEnumerator(c, ctx, storeID, args)
}

// consumeEnumerator opens a consume stream lazily on first MoveNext and keeps
// it for the lifetime of the logical stream. If that stream fails later, the
// error is surfaced directly and callers decide whether to retry from a saved
// offset or recreate the subscription.
type consumeEnumerator struct {
	client    *client
	storeID   uuid.UUID
	args      api.Routeable // ConsumeSegment or ConsumeSpace request
	ctx       context.Context
	cancel    context.CancelFunc
	innerEnum enumerators.Enumerator[api.Entry]
	current   *Entry
	err       error
}

func detachLongLivedContext(ctx context.Context) context.Context {
	detached := context.Background()
	if requestID, ok := telemetry.RequestIDFromContext(ctx); ok {
		detached = telemetry.WithRequestIDContext(detached, requestID)
	}
	if spanContext := trace.SpanContextFromContext(ctx); spanContext.IsValid() {
		detached = trace.ContextWithSpanContext(detached, spanContext)
	}
	return detached
}

func newDetachedCancelableContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, baseCancel := context.WithCancel(detachLongLivedContext(parent))
	stopParentCancel := context.AfterFunc(parent, baseCancel)
	return ctx, func() {
		stopParentCancel()
		baseCancel()
	}
}

func appendClientLogFields(fields []any, extras ...any) []any {
	out := make([]any, 0, len(fields)+len(extras))
	out = append(out, fields...)
	out = append(out, extras...)
	return out
}

func routeLogFields(storeID uuid.UUID, msg api.Routeable) []any {
	fields := []any{slog.String("store_id", storeID.String())}
	if msg == nil {
		return fields
	}

	fields = append(fields, slog.String("message_type", msg.GetDiscriminator()))
	switch args := msg.(type) {
	case *api.ConsumeSegment:
		fields = append(fields,
			slog.String("space", args.Space),
			slog.String("segment", args.Segment))
	case *api.ConsumeSpace:
		fields = append(fields, slog.String("space", args.Space))
	case *api.Consume:
		fields = append(fields, slog.Int("space_count", len(args.Offsets)))
	case *api.SubscribeToSegmentStatus:
		fields = append(fields,
			slog.String("space", args.Space),
			slog.String("segment", args.Segment))
	case *api.Produce:
		fields = append(fields,
			slog.String("space", args.Space),
			slog.String("segment", args.Segment))
	}

	return fields
}

func subscriptionLogFields(sub *activeSubscription) []any {
	fields := []any{
		slog.String("subscription_id", sub.id),
		slog.String("store_id", sub.storeID.String()),
	}
	if sub.initMsg == nil {
		return fields
	}

	fields = append(fields, slog.String("message_type", sub.initMsg.GetDiscriminator()))
	if args, ok := sub.initMsg.(*api.SubscribeToSegmentStatus); ok {
		fields = append(fields,
			slog.String("space", args.Space),
			slog.String("segment", args.Segment))
	}
	return fields
}

func subscriptionStatusLogFields(sub *activeSubscription, status *SegmentStatus) []any {
	fields := subscriptionLogFields(sub)
	if status != nil {
		fields = append(fields,
			slog.String("space", status.Space),
			slog.String("segment", status.Segment),
			slog.Uint64("last_sequence", status.LastSequence))
	}
	if lastDelivered := sub.lastDelivered.Load(); lastDelivered > 0 {
		fields = append(fields, slog.Int64("last_delivered_sequence", lastDelivered))
	}
	return fields
}

func leaseLogFields(l *leaseImpl) []any {
	return []any{
		slog.String("store_id", l.storeID.String()),
		slog.String("key", l.key),
		slog.String("holder", l.holder),
		slog.Duration("ttl", l.ttl),
	}
}

func produceLogFields(storeID uuid.UUID, space, segment string) []any {
	return []any{
		slog.String("store_id", storeID.String()),
		slog.String("space", space),
		slog.String("segment", segment),
	}
}

func ensureRequestIDContext(ctx context.Context) context.Context {
	if _, ok := telemetry.RequestIDFromContext(ctx); ok {
		return ctx
	}
	return telemetry.WithRequestIDContext(ctx, uuid.New())
}

func configuredSubscriptionHeartbeatSeconds() int64 {
	if subscriptionHeartbeatInterval <= 0 {
		return 0
	}
	seconds := int64(subscriptionHeartbeatInterval / time.Second)
	if subscriptionHeartbeatInterval%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		seconds = 1
	}
	return seconds
}

func subscriptionUsesPerSegmentPending(msg api.Routeable) bool {
	args, ok := msg.(*api.SubscribeToSegmentStatus)
	return ok && args != nil && args.Segment == "*"
}

func effectiveSubscriptionHeartbeatTimeout(msg api.Routeable) time.Duration {
	args, ok := msg.(*api.SubscribeToSegmentStatus)
	if !ok || args == nil || args.HeartbeatIntervalSeconds <= 0 {
		return 0
	}
	if subscriptionHeartbeatTimeout > 0 {
		return subscriptionHeartbeatTimeout
	}
	return 3 * time.Duration(args.HeartbeatIntervalSeconds) * time.Second
}

func stopTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

type subscriptionDecodeResult struct {
	status SegmentStatus
	err    error
}

func startSubscriptionReader(ctx context.Context, stream api.BidiStream) (<-chan subscriptionDecodeResult, <-chan struct{}) {
	results := make(chan subscriptionDecodeResult)
	done := make(chan struct{})

	go func() {
		defer close(done)
		defer close(results)

		for {
			var status SegmentStatus
			err := stream.Decode(&status)

			select {
			case results <- subscriptionDecodeResult{status: status, err: err}:
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

func stopSubscriptionReader(cancel context.CancelFunc, stream api.BidiStream, err error, done <-chan struct{}) {
	cancel()
	stream.Close(err)
	<-done
}

func normalizeHandlerConcurrency(limit int) int {
	if limit < 1 {
		return 1
	}
	return limit
}

func defaultHandlerWorkerCount(limit int) int {
	workers := normalizeHandlerConcurrency(limit) * 4
	if workers < 16 {
		workers = 16
	}
	if workers > 256 {
		workers = 256
	}
	return workers
}

func (c *client) startHandlerWorkers() {
	workerCount := defaultHandlerWorkerCount(c.maxConcurrentHandlers)
	c.handlerWorkerCount = workerCount
	c.handlerTaskQueue = make(chan subscriptionHandlerTask, workerCount*4)
	workerCtx, cancel := context.WithCancel(context.Background())
	c.handlerWorkersCancel = cancel

	for range workerCount {
		c.handlerWorkersWg.Add(1)
		go func() {
			defer c.handlerWorkersWg.Done()
			for {
				select {
				case <-workerCtx.Done():
					return
				case task := <-c.handlerTaskQueue:
					c.runHandler(task.ctx, task.sub, task.subID, task.handler, &task.status)
					task.sub.releaseHandlerSlot()
					c.signalSubscriptionMailbox(task.sub)
					task.wg.Done()
				}
			}
		}()
	}
}

func (s *activeSubscription) tryAcquireHandlerSlot(limit int) bool {
	maxInFlight := int32(normalizeHandlerConcurrency(limit))
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

func (s *activeSubscription) releaseHandlerSlot() {
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

func (s *activeSubscription) queuePendingStatus(status SegmentStatus) bool {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if !s.pendingPerSegment {
		coalesced := s.pendingSingleSet
		s.pendingSingleStatus = status
		s.pendingSingleSet = true
		return coalesced
	}
	if s.pendingStatusByKey == nil {
		s.pendingStatusByKey = make(map[string]SegmentStatus)
	}

	key := pendingStatusKey(status)
	_, coalesced := s.pendingStatusByKey[key]
	s.pendingStatusByKey[key] = status
	if !coalesced {
		s.pendingOrder = append(s.pendingOrder, key)
	}
	return coalesced
}

func (s *activeSubscription) hasPendingStatus() bool {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if !s.pendingPerSegment {
		return s.pendingSingleSet
	}
	return len(s.pendingOrder) > 0
}

func (s *activeSubscription) pendingStatusCount() int {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if !s.pendingPerSegment {
		if s.pendingSingleSet {
			return 1
		}
		return 0
	}
	return len(s.pendingOrder)
}

func (s *activeSubscription) takePendingStatus() (SegmentStatus, bool) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if !s.pendingPerSegment {
		if !s.pendingSingleSet {
			return SegmentStatus{}, false
		}
		status := s.pendingSingleStatus
		s.pendingSingleSet = false
		s.pendingSingleStatus = SegmentStatus{}
		return status, true
	}
	for len(s.pendingOrder) > 0 {
		key := s.pendingOrder[0]
		s.pendingOrder = s.pendingOrder[1:]

		status, ok := s.pendingStatusByKey[key]
		if !ok {
			continue
		}
		delete(s.pendingStatusByKey, key)
		return status, true
	}
	return SegmentStatus{}, false
}

func (s *activeSubscription) restorePendingStatus(status SegmentStatus) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if !s.pendingPerSegment {
		s.pendingSingleStatus = status
		s.pendingSingleSet = true
		return
	}
	if s.pendingStatusByKey == nil {
		s.pendingStatusByKey = make(map[string]SegmentStatus)
	}

	key := pendingStatusKey(status)
	if _, exists := s.pendingStatusByKey[key]; !exists {
		s.pendingOrder = append([]string{key}, s.pendingOrder...)
	}
	s.pendingStatusByKey[key] = status
}

func pendingStatusKey(status SegmentStatus) string {
	return status.Space + "\x00" + status.Segment
}

func (c *client) signalSubscriptionMailbox(sub *activeSubscription) {
	select {
	case sub.mailboxCh <- struct{}{}:
	default:
	}
}

func (c *client) recordSubscriptionCoalesced(id string) {
	if recorder, ok := c.metrics.(subscriptionOverloadMetrics); ok {
		recorder.RecordSubscriptionCoalesced(id)
	}
}

func (c *client) enqueueSubscriptionStatus(ctx context.Context, sub *activeSubscription, status SegmentStatus) {
	coalesced := sub.queuePendingStatus(status)
	c.signalSubscriptionMailbox(sub)
	if !coalesced {
		return
	}

	total := sub.coalescedUpdates.Add(1)
	c.recordSubscriptionCoalesced(sub.id)
	if total == 1 || total%100 == 0 {
		slog.WarnContext(ctx, "subscription handler saturated; coalescing latest update",
			appendClientLogFields(subscriptionStatusLogFields(sub, &status),
				slog.Int64("coalesced_updates", total))...)
	}
}

func (c *client) tryStartSubscriptionHandler(ctx context.Context, sub *activeSubscription, subID string, handler func(*SegmentStatus), status SegmentStatus, wg *sync.WaitGroup) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}

	if !sub.tryAcquireHandlerSlot(c.maxConcurrentHandlers) {
		return false
	}

	wg.Add(1)
	task := subscriptionHandlerTask{
		ctx:     ctx,
		sub:     sub,
		subID:   subID,
		handler: handler,
		status:  status,
		wg:      wg,
	}

	select {
	case <-ctx.Done():
		sub.releaseHandlerSlot()
		wg.Done()
		return true
	case c.handlerTaskQueue <- task:
		return true
	default:
		sub.releaseHandlerSlot()
		wg.Done()
		return false
	}
}

func (c *client) drainPendingSubscriptionStatuses(ctx context.Context, sub *activeSubscription, subID string, handler func(*SegmentStatus), wg *sync.WaitGroup) {
	for {
		if ctx.Err() != nil {
			return
		}

		status, ok := sub.takePendingStatus()
		if !ok {
			return
		}

		if c.tryStartSubscriptionHandler(ctx, sub, subID, handler, status, wg) {
			continue
		}

		sub.restorePendingStatus(status)
		return
	}
}

func (e *consumeEnumerator) MoveNext() bool {
	if e == nil {
		return false
	}

	if e.innerEnum == nil {
		bidi, err := e.createStream()
		if err != nil {
			e.err = err
			slog.WarnContext(e.ctx, "consume enumerator: failed to create stream",
				appendClientLogFields(routeLogFields(e.storeID, e.args), "err", err)...)
			return false
		}
		e.innerEnum = api.NewStreamEnumerator[api.Entry](bidi)
	}

	start := time.Now()
	if !e.innerEnum.MoveNext() {
		e.err = e.innerEnum.Err()
		e.current = nil
		e.innerEnum.Dispose()
		e.innerEnum = nil
		return false
	}

	entry, err := e.innerEnum.Current()
	if err != nil {
		e.err = err
		e.current = nil
		fields := routeLogFields(e.storeID, e.args)
		slog.WarnContext(e.ctx, "consume enumerator: failed to get current entry",
			appendClientLogFields(fields, "err", err)...)
		e.innerEnum.Dispose()
		e.innerEnum = nil
		return false
	}
	if !isValidConsumeEntry(&entry) {
		e.err = errInvalidConsumeEntry
		e.current = nil
		e.innerEnum.Dispose()
		e.innerEnum = nil
		return false
	}

	e.current = &entry
	e.err = nil
	if e.client != nil && e.client.metrics != nil {
		e.client.metrics.RecordConsumeLatency(entry.Space, entry.Segment, time.Since(start))
	}
	return true
}

func (e *consumeEnumerator) Current() (*Entry, error) {
	if e == nil || e.current == nil {
		return nil, errors.New("no current entry")
	}
	return e.current, nil
}

func (e *consumeEnumerator) Dispose() {
	if e.cancel != nil {
		e.cancel()
	}
	if e.innerEnum != nil {
		e.innerEnum.Dispose()
		e.innerEnum = nil
	}
	e.current = nil
}

func (e *consumeEnumerator) createStream() (api.BidiStream, error) {
	var bidi api.BidiStream
	err := RetryWithBackoff(e.ctx, e.client.policy, func(retryCtx context.Context) error {
		var err error
		bidi, err = e.client.provider.CallStream(retryCtx, e.storeID, e.args)
		return err
	})
	return bidi, err
}

func (e *consumeEnumerator) Err() error {
	if e == nil {
		return nil
	}
	if e.err != nil {
		return e.err
	}
	if e.innerEnum != nil {
		return e.innerEnum.Err()
	}
	return nil
}

func isValidConsumeEntry(entry *Entry) bool {
	if entry == nil {
		return false
	}
	if entry.Space == "" || entry.Segment == "" {
		return false
	}
	if entry.Sequence == 0 || entry.Timestamp == 0 {
		return false
	}
	if entry.Payload == nil {
		return false
	}
	return true
}

func newConsumeEnumerator(client *client, ctx context.Context, storeID uuid.UUID, args api.Routeable) enumerators.Enumerator[*Entry] {
	enumCtx, cancel := newDetachedCancelableContext(ctx)

	return &consumeEnumerator{
		client:  client,
		storeID: storeID,
		args:    args,
		ctx:     enumCtx,
		cancel:  cancel,
	}
}

func (c *client) Peek(ctx context.Context, storeID uuid.UUID, space, segment string) (*Entry, error) {
	var bidi api.BidiStream
	err := RetryWithBackoff(ctx, c.policy, func(retryCtx context.Context) error {
		var err error
		bidi, err = c.provider.CallStream(retryCtx, storeID, &api.Peek{Space: space, Segment: segment})
		return err
	})
	if err != nil {
		return nil, err
	}
	defer bidi.Close(nil)

	entry := &api.Entry{}
	if err := bidi.Decode(entry); err != nil {
		return nil, err
	}
	return entry, nil
}

// ErrLeaseNotAcquired is returned when WithLease could not acquire the lease (e.g. another holder has it).
var ErrLeaseNotAcquired = errors.New("lease not acquired")

const minLeaseTTL = time.Second
const leaseRenewIntervalRatio = 2 // renew every ttl/2

func (c *client) acquireLease(ctx context.Context, storeID uuid.UUID, key string, ttl time.Duration) (api.Lease, error) {
	if ttl < minLeaseTTL {
		ttl = minLeaseTTL
	}
	// Round to nearest second so fractional seconds are not silently truncated (e.g. 1.5s -> 2s, not 1s).
	ttlRounded := ttl.Round(time.Second)
	ttlSec := int64(ttlRounded.Seconds())
	if ttlSec < 1 {
		ttlSec = 1
	}
	holder := uuid.New().String()

	var bidi api.BidiStream
	err := RetryWithBackoff(ctx, c.policy, func(retryCtx context.Context) error {
		var err error
		bidi, err = c.provider.CallStream(retryCtx, storeID, &api.LeaseAcquire{Key: key, Holder: holder, TTLSeconds: ttlSec})
		return err
	})
	if err != nil {
		return nil, err
	}
	var result api.LeaseResult
	if err := bidi.Decode(&result); err != nil {
		bidi.Close(nil)
		return nil, err
	}
	bidi.Close(nil)
	if !result.Ok {
		return nil, fmt.Errorf("%w: %s", ErrLeaseNotAcquired, result.Message)
	}

	leaseCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	l := &leaseImpl{
		client:  c,
		storeID: storeID,
		key:     key,
		holder:  holder,
		ttl:     ttlRounded, // use rounded TTL so renew interval matches server TTL
		ttlSec:  ttlSec,
		ctx:     leaseCtx,
		cancel:  cancel,
		done:    done,
	}
	go l.renewLoop()
	return l, nil
}

// WithLease acquires a lease, runs fn with a context canceled on lease loss or ctx cancellation, then releases.
func (c *client) WithLease(ctx context.Context, storeID uuid.UUID, key string, ttl time.Duration, fn func(context.Context) error) error {
	lease, err := c.acquireLease(ctx, storeID, key, ttl)
	if err != nil {
		return err
	}
	defer lease.Release()

	runCtx, runCancel := newDetachedCancelableContext(ctx)
	stopLeaseCancel := context.AfterFunc(lease.Context(), runCancel)
	defer stopLeaseCancel()
	defer runCancel()
	return fn(runCtx)
}

// leaseImpl holds lease state and runs a background renew loop until Release() is called.
type leaseImpl struct {
	client      *client
	storeID     uuid.UUID
	key         string
	holder      string
	ttl         time.Duration
	ttlSec      int64
	ctx         context.Context
	cancel      context.CancelFunc
	releaseOnce sync.Once
	done        chan struct{}
}

func (l *leaseImpl) Done() <-chan struct{} { return l.done }

func (l *leaseImpl) Context() context.Context { return l.ctx }

func (l *leaseImpl) Release() error {
	var releaseErr error
	l.releaseOnce.Do(func() {
		l.cancel()
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer releaseCancel()
		bidi, err := l.client.provider.CallStream(releaseCtx, l.storeID, &api.LeaseRelease{Key: l.key, Holder: l.holder})
		if err != nil {
			releaseErr = err
			slog.Warn("lease release: call stream failed",
				appendClientLogFields(leaseLogFields(l), "err", err)...)
		} else {
			var res api.LeaseResult
			_ = bidi.Decode(&res)
			bidi.Close(nil)
		}
		close(l.done)
	})
	return releaseErr
}

func (l *leaseImpl) renewLoop() {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("lease renew loop panic",
				appendClientLogFields(leaseLogFields(l), slog.Any("panic", r))...)
		}
	}()
	interval := l.ttl / leaseRenewIntervalRatio
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			bidi, err := l.client.provider.CallStream(l.ctx, l.storeID, &api.LeaseRenew{Key: l.key, Holder: l.holder, TTLSeconds: l.ttlSec})
			if err != nil {
				if l.ctx.Err() == nil {
					slog.Warn("lease renew: call stream failed",
						appendClientLogFields(leaseLogFields(l), "err", err)...)
				}
				continue
			}
			var result api.LeaseResult
			if err := bidi.Decode(&result); err != nil {
				bidi.Close(nil)
				if l.ctx.Err() == nil {
					slog.Warn("lease renew: decode failed",
						appendClientLogFields(leaseLogFields(l), "err", err)...)
				}
				continue
			}
			bidi.Close(nil)
			if !result.Ok && l.ctx.Err() == nil {
				slog.Warn("lease renew: server returned not ok",
					appendClientLogFields(leaseLogFields(l), slog.String("message", result.Message))...)
				l.cancel() // cancel lease context so WithLease callback sees lease lost
			}
		}
	}
}

func (c *client) Produce(ctx context.Context, storeID uuid.UUID, space, segment string, entries enumerators.Enumerator[*Record]) enumerators.Enumerator[*SegmentStatus] {
	var bidi api.BidiStream
	err := RetryWithBackoff(ctx, c.policy, func(retryCtx context.Context) error {
		var err error
		bidi, err = c.provider.CallStream(retryCtx, storeID, &api.Produce{Space: space, Segment: segment})
		return err
	})
	if err != nil {
		return enumerators.Error[*SegmentStatus](err)
	}

	// Issue #4 & #10 FIX: Add panic recovery to produce goroutine
	go func(bidi api.BidiStream, entries enumerators.Enumerator[*Record]) {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("produce goroutine panic",
					appendClientLogFields(produceLogFields(storeID, space, segment), slog.Any("panic", r))...)
				bidi.Close(fmt.Errorf("panic in produce: %v", r))
			}
		}()
		defer entries.Dispose()
		count := 0
		for entries.MoveNext() {
			entry, err := entries.Current()
			if err != nil {
				slog.Error("produce: failed to get current entry",
					appendClientLogFields(produceLogFields(storeID, space, segment),
						slog.Int("count", count),
						"err", err)...)
				bidi.CloseSend(err)
				return
			}
			if err := bidi.Encode(entry); err != nil {
				slog.Error("produce: failed to encode entry",
					appendClientLogFields(produceLogFields(storeID, space, segment),
						slog.Int("count", count),
						"err", err)...)
				bidi.CloseSend(err)
				return
			}
			count++
		}
		bidi.CloseSend(entries.Err())
	}(bidi, entries)

	return api.NewStreamEnumerator[*SegmentStatus](bidi)
}

// Maximum number of per-segment produce locks to retain in the map.
// Tunable for high-churn scenarios. Tests may override for validation.
var MaxProduceLocks = 10000

// getProduceLock returns or creates a mutex for the given segment (Issue 9: Peek+Produce atomicity).
// This ensures that only one goroutine can perform Peek+Produce for a given segment,
// preventing race conditions where sequences could conflict.
// Issue #7 FIX: Removed double-check locking anti-pattern. Now use single lock acquire.
func (c *client) getProduceLock(storeID uuid.UUID, space, segment string) *sync.Mutex {
	key := fmt.Sprintf("%s:%s:%s", storeID.String(), space, segment)

	// Acquire write lock upfront to avoid TOCTOU race where fast-path returns a lock
	// that gets deleted by slow-path eviction
	c.produceLocksLock.Lock()
	defer c.produceLocksLock.Unlock()

	// Check if lock already exists
	if mu, ok := c.produceLocks[key]; ok {
		return mu
	}

	// Evict old locks if map is too large. Only remove entries whose mutex we can lock,
	// so we never delete a mutex that another goroutine is holding (preserves mutual exclusion).
	if len(c.produceLocks) >= MaxProduceLocks {
		toRemove := MaxProduceLocks / 10
		for k := range c.produceLocks {
			if toRemove <= 0 {
				break
			}
			mu := c.produceLocks[k]
			if mu.TryLock() {
				delete(c.produceLocks, k)
				mu.Unlock()
				toRemove--
			}
		}
	}

	// Create and store new mutex
	mu := &sync.Mutex{}
	c.produceLocks[key] = mu
	return mu
}

func (c *client) Publish(ctx context.Context, storeID uuid.UUID, space, segment string, payload []byte, metadata map[string]string) error {
	start := time.Now()
	defer func() {
		if c.metrics != nil {
			c.metrics.RecordProduceLatency(space, segment, time.Since(start))
		}
	}()

	// Issue 9: Acquire segment lock to ensure atomic Peek+Produce operation.
	// This prevents race conditions where another producer could write data between
	// our Peek and Produce, causing sequence conflicts.
	segmentLock := c.getProduceLock(storeID, space, segment)
	segmentLock.Lock()
	defer segmentLock.Unlock()

	peek, err := c.Peek(ctx, storeID, space, segment)
	if err != nil {
		return err
	}
	// If peek returns sequence 0, retry a few times to handle transient peek anomalies.
	for i := 0; i < 3 && peek.Sequence == 0; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
			if err := ctx.Err(); err != nil {
				return err
			}
			peek, err = c.Peek(ctx, storeID, space, segment)
			if err != nil {
				return err
			}
		}
	}

	var bidi api.BidiStream
	err = RetryWithBackoff(ctx, c.policy, func(retryCtx context.Context) error {
		var err error
		bidi, err = c.provider.CallStream(retryCtx, storeID, &api.Produce{Space: space, Segment: segment})
		return err
	})
	if err != nil {
		return err
	}
	defer bidi.Close(nil)

	record := &api.Record{
		Sequence: peek.Sequence + 1,
		Payload:  payload,
		Metadata: metadata,
	}

	if err := bidi.Encode(record); err != nil {
		bidi.CloseSend(err)
		return err
	}
	bidi.CloseSend(nil)

	// Issue #2 (CRITICAL): Wait for server confirmation before returning.
	// Previously this was fire-and-forget, causing Publish to return success
	// even when the server failed (data loss in event sourcing).
	enumerator := api.NewStreamEnumerator[*SegmentStatus](bidi)
	defer enumerator.Dispose()

	// Wait for at least one status confirmation from the server
	if !enumerator.MoveNext() {
		if err := enumerator.Err(); err != nil {
			return fmt.Errorf("produce failed: %w", err)
		}
		return fmt.Errorf("no status confirmation from server")
	}

	return nil
}

func (c *client) SubscribeToSpace(ctx context.Context, storeID uuid.UUID, space string, handler func(*SegmentStatus)) (api.Subscription, error) {
	args := &api.SubscribeToSegmentStatus{
		Space:                    space,
		Segment:                  "*",
		HeartbeatIntervalSeconds: configuredSubscriptionHeartbeatSeconds(),
	}
	return c.subscribeStream(ctx, storeID, args, handler)
}

func (c *client) SubscribeToSegment(ctx context.Context, storeID uuid.UUID, space, segment string, handler func(*SegmentStatus)) (api.Subscription, error) {
	args := &api.SubscribeToSegmentStatus{
		Space:                    space,
		Segment:                  segment,
		HeartbeatIntervalSeconds: configuredSubscriptionHeartbeatSeconds(),
	}
	return c.subscribeStream(ctx, storeID, args, handler)
}

func (c *client) subscribeStream(ctx context.Context, storeID uuid.UUID, initMsg api.Routeable, handler func(*SegmentStatus)) (api.Subscription, error) {
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

	// Generate unique subscription ID for tracking
	subID := uuid.New().String()

	activeSub := &activeSubscription{
		id:                subID,
		storeID:           storeID,
		initMsg:           initMsg,
		handler:           handler,
		cancel:            cancel,
		stopped:           make(chan struct{}),
		done:              done,
		ctx:               subCtx,
		mailboxCh:         make(chan struct{}, 1),
		pendingPerSegment: subscriptionUsesPerSegmentPending(initMsg),
	}

	activeSub.status.Store("active")
	c.registerSubscription(subID, activeSub)

	go func(stream api.BidiStream) {
		subCtx := activeSub.ctx
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(subCtx, "subscription goroutine panic (cleanup completed)",
					appendClientLogFields(subscriptionLogFields(activeSub), slog.Any("panic", r))...)
			}
		}()

		var handlerWg sync.WaitGroup
		defer handlerWg.Wait()
		defer close(activeSub.stopped)
		defer c.unregisterSubscription(subID)

		heartbeatTimeout := effectiveSubscriptionHeartbeatTimeout(activeSub.initMsg)
		lastFrameAt := time.Now()
		streamReadCtx, cancelStreamRead := context.WithCancel(subCtx)
		decodeCh, readerDone := startSubscriptionReader(streamReadCtx, stream)

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
				stopSubscriptionReader(cancelStreamRead, stream, subCtx.Err(), readerDone)
				return
			case <-activeSub.mailboxCh:
				stopTimer(timer)
				c.drainPendingSubscriptionStatuses(subCtx, activeSub, subID, handler, &handlerWg)
				continue
			case <-timeoutCh:
				stopTimer(timer)
				timeoutErr := fmt.Errorf("%w after %s", errSubscriptionHeartbeatLost, heartbeatTimeout)
				activeSub.failureCount.Add(1)
				activeSub.status.Store("failed")
				activeSub.setLastError(timeoutErr)
				slog.WarnContext(subCtx, "client: subscription heartbeat timed out",
					appendClientLogFields(subscriptionLogFields(activeSub),
						slog.Int("failure_count", int(activeSub.failureCount.Load())),
						slog.Duration("heartbeat_timeout", heartbeatTimeout),
						"err", timeoutErr)...)
				stopSubscriptionReader(cancelStreamRead, stream, timeoutErr, readerDone)
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
					slog.WarnContext(subCtx, "client: subscription reader stopped unexpectedly",
						appendClientLogFields(subscriptionLogFields(activeSub),
							slog.Int("failure_count", int(activeSub.failureCount.Load())),
							"err", streamErr)...)
					stopSubscriptionReader(cancelStreamRead, stream, streamErr, readerDone)
					return
				}
				if result.err != nil {
					activeSub.failureCount.Add(1)
					activeSub.status.Store("failed")
					activeSub.setLastError(result.err)
					slog.WarnContext(subCtx, "client: subscription stream ended",
						appendClientLogFields(subscriptionLogFields(activeSub),
							slog.Int("failure_count", int(activeSub.failureCount.Load())),
							"err", result.err)...)
					stopSubscriptionReader(cancelStreamRead, stream, result.err, readerDone)
					return
				}

				lastFrameAt = time.Now()
				if result.status.Heartbeat {
					continue
				}

				if !c.tryStartSubscriptionHandler(subCtx, activeSub, subID, handler, result.status, &handlerWg) {
					c.enqueueSubscriptionStatus(subCtx, activeSub, result.status)
				}
				c.drainPendingSubscriptionStatuses(subCtx, activeSub, subID, handler, &handlerWg)
			}
		}
	}(stream)

	return &subscription{id: subID, cancel: cancel, done: done}, nil
}

func (c *client) runHandlerSafely(ctx context.Context, sub *activeSubscription, subID string, handler func(*SegmentStatus), status *SegmentStatus) {
	defer func() {
		if r := recover(); r != nil {
			sub.handlerPanics.Add(1)
			if c.metrics != nil {
				c.metrics.RecordHandlerPanic(subID)
			}
			slog.ErrorContext(ctx, "subscription handler panic",
				appendClientLogFields(subscriptionStatusLogFields(sub, status), slog.Any("panic", r))...)
		}
	}()
	handler(status)
}

// runHandler executes a subscription handler with optional timeout and panic recovery.
func (c *client) runHandler(ctx context.Context, sub *activeSubscription, subID string, handler func(*SegmentStatus), status *SegmentStatus) {
	if status.LastSequence > 0 {
		sub.lastDelivered.Store(int64(status.LastSequence))
	}

	if c.handlerTimeout <= 0 {
		c.runHandlerSafely(ctx, sub, subID, handler, status)
		return
	}

	handlerCtx, cancel := context.WithTimeout(ctx, c.handlerTimeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.runHandlerSafely(ctx, sub, subID, handler, status)
	}()

	select {
	case <-done:
	case <-handlerCtx.Done():
		if !errors.Is(handlerCtx.Err(), context.DeadlineExceeded) {
			return
		}
		sub.handlerTimeouts.Add(1)
		if c.metrics != nil {
			c.metrics.RecordHandlerTimeout(subID)
		}
		slog.WarnContext(ctx, "subscription handler exceeded timeout",
			appendClientLogFields(subscriptionStatusLogFields(sub, status),
				slog.Duration("timeout", c.handlerTimeout),
				slog.Int("timeout_count", int(sub.handlerTimeouts.Load())))...)
	}
}

type subscription struct {
	id     string
	cancel func()
	done   <-chan struct{}
}

func (s *subscription) ID() string {
	if s == nil {
		return ""
	}
	return s.id
}

func (s *subscription) Unsubscribe() {
	s.cancel()
	<-s.done
}

// OnReconnected is kept for compatibility with provider reconnect listener
// registration, but the client no longer attempts to resume active logical
// streams when a WebSocket reconnects.
func (c *client) OnReconnected(ctx context.Context, storeID uuid.UUID) error {
	slog.DebugContext(ctx, "client: provider reconnected; active logical streams must be recreated by callers",
		slog.String("store_id", storeID.String()))
	return nil
}

// Close gracefully shuts down the client and cancels active subscriptions.
// Safe to call multiple times.
func (c *client) Close() error {
	c.shutdownOnce.Do(func() {
		for {
			subs := c.snapshotSubscriptions()

			if len(subs) == 0 {
				break
			}

			for _, sub := range subs {
				sub.cancel()
			}
			for _, sub := range subs {
				<-sub.stopped
			}
		}

		if c.handlerWorkersCancel != nil {
			c.handlerWorkersCancel()
		}
		c.handlerWorkersWg.Wait()
	})

	return nil
}

// registerSubscription adds a subscription to the active registry.
func (c *client) registerSubscription(id string, sub *activeSubscription) {
	shard := c.subscriptionShard(id)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.subscriptions[id] = sub
}

// unregisterSubscription removes a subscription from the registry
func (c *client) unregisterSubscription(id string) {
	shard := c.subscriptionShard(id)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	delete(shard.subscriptions, id)
}

// RemoveSubscription explicitly removes a subscription from the registry.
// Returns true if the subscription existed and was removed.
func (c *client) RemoveSubscription(id string) bool {
	shard := c.subscriptionShard(id)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	_, existed := shard.subscriptions[id]
	delete(shard.subscriptions, id)
	return existed
}

// RetryFailedSubscription is not supported for non-resumable subscriptions.
// Callers must create a new subscription instead.
func (c *client) RetryFailedSubscription(id string) error {
	_, exists := c.getSubscription(id)

	if !exists {
		return fmt.Errorf("subscription %s not found", id)
	}

	return fmt.Errorf("subscription %s does not support in-place retry; create a new subscription", id)
}

// SubscriptionStatus represents the health status of an active subscription.
type SubscriptionStatus struct {
	ID               string // subscription ID
	Status           string // "active" or "failed"
	FailureCount     int32  // stream failures observed before termination
	LastError        error  // last stream error observed before termination
	LastSequence     int64  // last successfully delivered sequence
	HandlerTimeouts  int32  // count of handler timeout occurrences
	HandlerPanics    int32  // count of handler panic occurrences
	CoalescedUpdates int64  // count of updates merged while handlers were saturated
	PendingStatuses  int    // distinct latest statuses currently queued for delivery
}

// GetSubscriptionStatus returns the current health status of a subscription.
//
// The returned status contains the subscription's current state while it is
// still registered. Active subscriptions report "active"; a stream that has
// observed a terminal failure may briefly report "failed" until cleanup removes
// it from the registry. Returns nil if the subscription id is not found or the
// subscription has already terminated.
func (c *client) GetSubscriptionStatus(id string) *SubscriptionStatus {
	sub, exists := c.getSubscription(id)

	if !exists {
		return nil
	}

	status := "active"
	if s, ok := sub.status.Load().(string); ok {
		status = s
	}

	return &SubscriptionStatus{
		ID:               sub.id,
		Status:           status,
		FailureCount:     sub.failureCount.Load(),
		LastError:        sub.loadLastError(),
		LastSequence:     sub.lastDelivered.Load(),
		HandlerTimeouts:  sub.handlerTimeouts.Load(),
		HandlerPanics:    sub.handlerPanics.Load(),
		CoalescedUpdates: sub.coalescedUpdates.Load(),
		PendingStatuses:  sub.pendingStatusCount(),
	}
}
