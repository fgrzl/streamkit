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
	"github.com/fgrzl/lexkey"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/telemetry"
	"github.com/google/uuid"
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

	// AcquireLease acquires a lease for the given key with the given TTL. The returned Lease
	// auto-renews in the background until Release() is called. Release() is idempotent.
	AcquireLease(ctx context.Context, storeID uuid.UUID, key string, ttl time.Duration) (api.Lease, error)

	// Close gracefully shuts down the client, stopping background goroutines and cleaning up resources.
	// Safe to call multiple times. Should be called when the client is no longer needed.
	Close() error
}

// NewClient creates a new Client instance using the provided BidiStreamProvider.
// The client includes built-in resilience for transient failures and automatic
// subscription replay on provider reconnection.
// Handler timeout is set to 30 seconds by default (Issue 7 - prevents slow handlers from blocking).
func NewClient(provider api.BidiStreamProvider) Client {
	return NewClientWithHandlerTimeout(provider, 30*time.Second)
}

// NewClientWithRetryPolicy creates a new Client with a custom retry policy.
func NewClientWithRetryPolicy(provider api.BidiStreamProvider, policy RetryPolicy) Client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		provider:              provider,
		policy:                policy,
		handlerTimeout:        30 * time.Second,
		handlerQueueSize:      500,
		maxConcurrentHandlers: 50,
		subscriptions:         make(map[string]*activeSubscription),
		reconnectQueue:        make(chan *reconnectRequest, 8192),
		reconnectCtx:          ctx,
		reconnectCancel:       cancel,
		produceLocks:          make(map[string]*sync.Mutex),
	}
	c.startReconnectDispatcher()
	provider.RegisterReconnectListener(c)
	return c
}

// NewClientWithHandlerTimeout creates a new Client with custom timeout for handler execution (Issue 7).
// Prevents slow handlers from blocking the subscription loop and starving other subscriptions.
// If a handler takes longer than the specified timeout, it is interrupted and execution continues
// with the next message. The timeout event is logged and tracked in subscription metrics.
func NewClientWithHandlerTimeout(provider api.BidiStreamProvider, handlerTimeout time.Duration) Client {
	return NewClientWithMetrics(provider, handlerTimeout, nil)
}

// NewClientWithMetrics creates a new Client with optional metrics. Pass nil for metrics to disable.
// Use NewOTelClientMetrics() for OpenTelemetry-backed produce/consume latency and
// subscription replay/timeout/panic metrics.
func NewClientWithMetrics(provider api.BidiStreamProvider, handlerTimeout time.Duration, metrics ClientMetrics) Client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		provider:              provider,
		policy:                DefaultRetryPolicy(),
		handlerTimeout:        handlerTimeout,
		handlerQueueSize:      500,
		maxConcurrentHandlers: 50,
		subscriptions:         make(map[string]*activeSubscription),
		reconnectQueue:        make(chan *reconnectRequest, 8192),
		reconnectCtx:          ctx,
		reconnectCancel:       cancel,
		produceLocks:          make(map[string]*sync.Mutex),
		metrics:               metrics,
	}

	c.startReconnectDispatcher()
	provider.RegisterReconnectListener(c)

	return c
}

// activeSubscription represents a subscription managed by a single goroutine.
// The goroutine retries indefinitely with exponential backoff until cancelled.
type activeSubscription struct {
	id              string               // unique subscription ID
	storeID         uuid.UUID            // store being subscribed to
	initMsg         api.Routeable        // initial subscription message
	handler         func(*SegmentStatus) // handler to call for each status update
	cancel          context.CancelFunc   // cancels the subscription goroutine
	done            <-chan struct{}      // signals when subscription has stopped
	lastDelivered   atomic.Int64         // tracks last delivered sequence for offset resumption
	ctx             context.Context      // subscription's own context
	failureCount    atomic.Int32         // tracks consecutive failures for observability
	status          atomic.Value         // current status: "active"|"reconnecting"
	lastError       atomic.Value         // stores last error encountered
	handlerTimeouts atomic.Int32         // counts handler timeout occurrences
	handlerPanics   atomic.Int32         // counts handler panic occurrences
	handlerCh       chan SegmentStatus   // buffered dispatch channel for non-blocking handler delivery
	streamCh        chan api.BidiStream  // receives a reconnected stream from the dispatcher
	retryRequested  atomic.Bool          // set by RetryFailedSubscription to break out and re-enqueue immediately
}

// reconnectRequest is sent by a subscription goroutine to enqueue itself for reconnection.
type reconnectRequest struct {
	sub *activeSubscription
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

type client struct {
	provider              api.BidiStreamProvider
	policy                RetryPolicy
	handlerTimeout        time.Duration // max time allowed for handler to execute (Issue 7)
	handlerQueueSize      int           // buffer size for per-subscription handler dispatch channel
	maxConcurrentHandlers int           // worker goroutines per subscription for handler execution

	// subscription registry for tracking active subscriptions
	subscriptionsMu sync.RWMutex
	subscriptions   map[string]*activeSubscription

	// Single-threaded reconnection: subscription goroutines enqueue themselves here
	// and a single dispatcher loop calls CallStream serially for each one.
	reconnectQueue  chan *reconnectRequest
	reconnectLoop   sync.Once
	reconnectCtx    context.Context
	reconnectCancel context.CancelFunc
	shutdownOnce    sync.Once

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
	ctx = telemetry.WithRequestIDContext(ctx, uuid.New())
	return newResilienceEnumerator(c, ctx, storeID, args)
}

func (c *client) ConsumeSegment(ctx context.Context, storeID uuid.UUID, args *api.ConsumeSegment) enumerators.Enumerator[*Entry] {
	ctx = telemetry.WithRequestIDContext(ctx, uuid.New())
	return newResilienceEnumerator(c, ctx, storeID, args)
}

func (c *client) Consume(ctx context.Context, storeID uuid.UUID, args *api.Consume) enumerators.Enumerator[*Entry] {
	ctx = telemetry.WithRequestIDContext(ctx, uuid.New())
	return newResilienceEnumerator(c, ctx, storeID, args)
}

// resilienceEnumerator wraps a consume enumerator with transparent reconnection (Issue 8).
// On reconnect, the enumerator will resume from the last consumed position to provide
// at-least-once semantics. It updates Consume arguments to precisely resume using
// lexicographic offsets when available to avoid duplicate deliveries on reconnect.
type resilienceEnumerator struct {
	client    *client
	storeID   uuid.UUID
	args      api.Routeable // ConsumeSegment or ConsumeSpace request
	ctx       context.Context
	innerEnum enumerators.Enumerator[*Entry]

	lastEntry      *Entry
	attemptCount   int
	maxAttempts    int
	disconnectedAt time.Time
}

func (e *resilienceEnumerator) MoveNext() bool {
	start := time.Now()
	for {
		// Create inner enumerator if not exists
		if e.innerEnum == nil {
			bidi, err := e.createStream()
			if err != nil {
				slog.WarnContext(e.ctx, "resilience enumerator: failed to create stream", "err", err, "attempt", e.attemptCount)
				e.attemptCount++
				if e.attemptCount >= e.maxAttempts {
					return false
				}
				// Issue #8 FIX: Check context before backoff sleep to respect cancellation
				if err := e.ctx.Err(); err != nil {
					return false
				}
				// Backoff before retrying
				select {
				case <-e.ctx.Done():
					return false
				case <-time.After(time.Second * time.Duration(e.attemptCount)):
				}
				continue
			}
			e.innerEnum = api.NewStreamEnumerator[*Entry](bidi)
			e.disconnectedAt = time.Time{}
			e.attemptCount = 0
		}

		// Try to read next entry
		if e.innerEnum.MoveNext() {
			entry, err := e.innerEnum.Current()
			if err != nil {
				slog.WarnContext(e.ctx, "resilience enumerator: failed to get current entry", "err", err)
				// Close this enumerator and retry
				if e.innerEnum != nil {
					e.innerEnum.Dispose()
					e.innerEnum = nil
				}
				e.attemptCount++
				if e.attemptCount >= e.maxAttempts {
					return false
				}
				continue
			}
			e.lastEntry = entry
			// Metrics: record consume latency if available
			if e.client != nil && e.client.metrics != nil {
				e.client.metrics.RecordConsumeLatency(entry.Space, entry.Segment, time.Since(start))
			}
			return true
		}

		// Stream ended - check if it was an error or clean EOF
		var streamErr error
		if e.innerEnum != nil {
			streamErr = e.innerEnum.Err()
			e.innerEnum.Dispose()
			e.innerEnum = nil
		}

		// Only retry if there was an actual error (disconnect), not on clean EOF
		if streamErr != nil && e.lastEntry != nil {
			if e.attemptCount < e.maxAttempts {
				e.updateConsumePosition()
				e.attemptCount++
				e.disconnectedAt = time.Now()
				continue
			}
		}

		// No more data or max attempts reached
		return false
	}
}

func (e *resilienceEnumerator) Current() (*Entry, error) {
	if e.lastEntry == nil {
		return nil, errors.New("no current entry")
	}
	return e.lastEntry, nil
}

func (e *resilienceEnumerator) Dispose() {
	if e.innerEnum != nil {
		e.innerEnum.Dispose()
		e.innerEnum = nil
	}
}

// createStream creates a new stream for the consume operation
func (e *resilienceEnumerator) createStream() (api.BidiStream, error) {
	// If we have consumed entries previously, update the args to resume from
	// the position after the last consumed entry to prevent duplicates.
	argsToUse := e.args
	if e.lastEntry != nil {
		argsToUse = e.updateArgsWithOffset()
	}

	var bidi api.BidiStream
	err := RetryWithBackoff(e.ctx, e.client.policy, func(retryCtx context.Context) error {
		var err error
		bidi, err = e.client.provider.CallStream(retryCtx, e.storeID, argsToUse)
		return err
	})
	return bidi, err
}

// updateConsumePosition updates the consume arguments with the position after last entry
func (e *resilienceEnumerator) updateConsumePosition() {
	// Update based on type of consume argument
	switch args := e.args.(type) {
	case *api.ConsumeSegment:
		// Resume from next sequence after last consumed (Issue 8)
		if e.lastEntry != nil && e.lastEntry.Sequence > 0 {
			args.MinSequence = e.lastEntry.Sequence + 1
		}
	case *api.ConsumeSpace:
		// For ConsumeSpace, we need to resume past the last timestamp
		// Add 1 nanosecond to avoid re-receiving entries with same timestamp
		if e.lastEntry != nil && e.lastEntry.Timestamp > 0 {
			args.MinTimestamp = e.lastEntry.Timestamp + 1
		}
	case *api.Consume:
		// For Consume (multi-space), update offset map with last seen position
		// If Offsets map exists, update the specific space/segment pair
		if e.lastEntry != nil && args.Offsets != nil {
			offsetKey := e.lastEntry.Space + "/" + e.lastEntry.Segment
			args.Offsets[offsetKey] = lexkey.Encode(e.lastEntry.Sequence + 1)
		}
	}
}

// build a new args object that resumes from the last consumed position. This is
// used when reconnecting so a fresh stream starts from the correct offset.
func (e *resilienceEnumerator) updateArgsWithOffset() api.Routeable {
	if e.lastEntry == nil {
		return e.args
	}

	switch args := e.args.(type) {
	case *api.ConsumeSegment:
		newArgs := *args
		newArgs.MinSequence = e.lastEntry.Sequence + 1
		return &newArgs
	case *api.ConsumeSpace:
		newArgs := *args
		newArgs.Offset = e.lastEntry.GetSpaceOffset()
		return &newArgs
	case *api.Consume:
		newArgs := *args
		if newArgs.Offsets == nil {
			newArgs.Offsets = make(map[string]lexkey.LexKey)
		}
		offsetKey := e.lastEntry.Space + "/" + e.lastEntry.Segment
		newArgs.Offsets[offsetKey] = lexkey.Encode(e.lastEntry.Timestamp, e.lastEntry.Space, e.lastEntry.Segment, e.lastEntry.Sequence)
		return &newArgs
	}

	return e.args
}

// Err returns any error that occurred during enumeration
func (e *resilienceEnumerator) Err() error {
	if e.innerEnum != nil {
		return e.innerEnum.Err()
	}
	return nil
}

// newResilienceEnumerator creates a resilience-wrapped enumerator (Issue 8)
func newResilienceEnumerator(client *client, ctx context.Context, storeID uuid.UUID, args api.Routeable) enumerators.Enumerator[*Entry] {
	return &resilienceEnumerator{
		client:       client,
		storeID:      storeID,
		args:         args,
		ctx:          ctx,
		maxAttempts:  7, // Same as subscription retry logic
		attemptCount: 0,
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

// ErrLeaseNotAcquired is returned when AcquireLease could not acquire the lease (e.g. another holder has it).
var ErrLeaseNotAcquired = errors.New("lease not acquired")

const minLeaseTTL = time.Second
const leaseRenewIntervalRatio = 2 // renew every ttl/2

func (c *client) AcquireLease(ctx context.Context, storeID uuid.UUID, key string, ttl time.Duration) (api.Lease, error) {
	if ttl < minLeaseTTL {
		ttl = minLeaseTTL
	}
	ttlSec := int64(ttl.Seconds())
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
		ttl:     ttl,
		ttlSec:  ttlSec,
		ctx:     leaseCtx,
		cancel:  cancel,
		done:    done,
	}
	go l.renewLoop()
	return l, nil
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

func (l *leaseImpl) Release() error {
	var releaseErr error
	l.releaseOnce.Do(func() {
		l.cancel()
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer releaseCancel()
		bidi, err := l.client.provider.CallStream(releaseCtx, l.storeID, &api.LeaseRelease{Key: l.key, Holder: l.holder})
		if err != nil {
			releaseErr = err
			slog.Warn("lease release: CallStream failed", "key", l.key, "err", err)
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
			slog.Error("lease renew loop panic", "key", l.key, "panic", r)
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
					slog.Warn("lease renew: CallStream failed", "key", l.key, "err", err)
				}
				continue
			}
			var result api.LeaseResult
			if err := bidi.Decode(&result); err != nil {
				bidi.Close(nil)
				if l.ctx.Err() == nil {
					slog.Warn("lease renew: Decode failed", "key", l.key, "err", err)
				}
				continue
			}
			bidi.Close(nil)
			if !result.Ok && l.ctx.Err() == nil {
				slog.Warn("lease renew: server returned not ok", "key", l.key, "message", result.Message)
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
				slog.Error("produce goroutine panic", "err", r)
				bidi.Close(fmt.Errorf("panic in produce: %v", r))
			}
		}()
		defer entries.Dispose()
		count := 0
		for entries.MoveNext() {
			entry, err := entries.Current()
			if err != nil {
				slog.Error("produce: failed to get current entry", "err", err, "count", count)
				bidi.CloseSend(err)
				return
			}
			if err := bidi.Encode(entry); err != nil {
				slog.Error("produce: failed to encode entry", "err", err, "count", count)
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
	args := &api.SubscribeToSegmentStatus{Space: space, Segment: "*"}
	return c.subscribeStream(ctx, storeID, args, handler)
}

func (c *client) SubscribeToSegment(ctx context.Context, storeID uuid.UUID, space, segment string, handler func(*SegmentStatus)) (api.Subscription, error) {
	args := &api.SubscribeToSegmentStatus{Space: space, Segment: segment}
	return c.subscribeStream(ctx, storeID, args, handler)
}

func (c *client) subscribeStream(ctx context.Context, storeID uuid.UUID, initMsg api.Routeable, handler func(*SegmentStatus)) (api.Subscription, error) {
	subCtx, cancel := context.WithCancel(ctx)
	// Tie subscription lifecycle to client shutdown so subscription goroutines exit before
	// Close() closes reconnectQueue (avoids send on closed channel panic).
	go func() {
		select {
		case <-subCtx.Done():
			return
		case <-c.reconnectCtx.Done():
			cancel()
		}
	}()
	done := make(chan struct{})

	// Generate unique subscription ID for tracking
	subID := uuid.New().String()

	// Create the activeSubscription for replaying on reconnect
	activeSub := &activeSubscription{
		id:        subID,
		storeID:   storeID,
		initMsg:   initMsg,
		handler:   handler,
		cancel:    cancel,
		done:      done,
		ctx:       subCtx,
		handlerCh: make(chan SegmentStatus, c.handlerQueueSize),
		streamCh:  make(chan api.BidiStream, 1),
	}

	// Initialize health status
	activeSub.status.Store("active")

	// Register subscription for replay on reconnect
	c.registerSubscription(subID, activeSub)

	go func() {
		subCtx := activeSub.ctx
		defer close(done)
		defer c.unregisterSubscription(subID)
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(subCtx, "subscription goroutine panic (cleanup completed)", "id", subID, "panic", r)
			}
		}()

		// Start a fixed pool of worker goroutines that process handler dispatches.
		// Workers survive stream reconnects and exit when handlerCh is closed.
		var workerWg sync.WaitGroup
		for i := 0; i < c.maxConcurrentHandlers; i++ {
			workerWg.Add(1)
			go func() {
				defer workerWg.Done()
				for status := range activeSub.handlerCh {
					c.runHandler(subCtx, activeSub, subID, handler, &status)
				}
			}()
		}
		// Defer order matters (LIFO): close channel first so workers exit, then wait for them.
		defer workerWg.Wait()
		defer close(activeSub.handlerCh)

		for {
			select {
			case <-subCtx.Done():
				return
			default:
			}

			// Enqueue ourselves for reconnection — the dispatcher calls CallStream serially.
			activeSub.status.Store("reconnecting")
			select {
			case <-subCtx.Done():
				return
			case c.reconnectQueue <- &reconnectRequest{sub: activeSub}:
			}

			// Wait for the dispatcher to hand us a stream (or cancellation).
			var stream api.BidiStream
			select {
			case <-subCtx.Done():
				return
			case stream = <-activeSub.streamCh:
			}

			if stream == nil {
				// Dispatcher could not create stream; loop back to re-enqueue.
				continue
			}

			// Connected successfully — reset failure state
			activeSub.failureCount.Store(0)
			activeSub.status.Store("active")

			// Read from stream until it fails, context is cancelled, or retry requested
			for {
				if activeSub.retryRequested.Swap(false) {
					stream.Close(nil)
					break
				}
				select {
				case <-subCtx.Done():
					stream.Close(subCtx.Err())
					return
				default:
				}

				var status SegmentStatus
				if err := stream.Decode(&status); err != nil {
					activeSub.failureCount.Add(1)
					activeSub.lastError.Store(err)
					stream.Close(err)
					break // inner loop → re-enqueue for reconnection
				}

				if activeSub.failureCount.Load() > 0 {
					activeSub.failureCount.Store(0)
				}

				select {
				case activeSub.handlerCh <- status:
				default:
				}
			}
		}
	}()

	return &subscription{cancel: cancel, done: done}, nil
}

// runHandler executes a subscription handler with timeout and panic recovery.
func (c *client) runHandler(ctx context.Context, sub *activeSubscription, subID string, handler func(*SegmentStatus), status *SegmentStatus) {
	if status.LastSequence > 0 {
		sub.lastDelivered.Store(int64(status.LastSequence))
	}

	handlerCtx, cancel := context.WithTimeout(ctx, c.handlerTimeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				sub.handlerPanics.Add(1)
				if c.metrics != nil {
					c.metrics.RecordHandlerPanic(subID)
				}
				slog.ErrorContext(ctx, "subscription handler panic", "id", subID, "panic", r)
			}
		}()
		handler(status)
	}()

	select {
	case <-done:
	case <-handlerCtx.Done():
		sub.handlerTimeouts.Add(1)
		if c.metrics != nil {
			c.metrics.RecordHandlerTimeout(subID)
		}
		slog.WarnContext(ctx, "subscription handler exceeded timeout", "id", subID, "timeout", c.handlerTimeout)
	}
}

type subscription struct {
	cancel func()
	done   <-chan struct{}
}

func (s *subscription) Unsubscribe() {
	s.cancel()
	<-s.done
}

// startReconnectDispatcher starts a single-threaded goroutine that processes
// reconnection requests one at a time. Subscription goroutines enqueue themselves
// here when their stream dies; the dispatcher calls CallStream serially and delivers
// the resulting stream back, preventing thundering herd on reconnect.
func (c *client) startReconnectDispatcher() {
	c.reconnectLoop.Do(func() {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("reconnect dispatcher panic", "err", r)
				}
			}()

			backoff := time.Duration(0)
			maxBackoff := 30 * time.Second

			for {
				select {
				case <-c.reconnectCtx.Done():
					return
				case req, ok := <-c.reconnectQueue:
					if !ok {
						return
					}

					sub := req.sub
					replayStart := time.Now()

					// Skip if subscription was cancelled while waiting in queue
					select {
					case <-sub.ctx.Done():
						continue
					default:
					}

					// Backoff between consecutive CallStream attempts
					if backoff > 0 {
						select {
						case <-c.reconnectCtx.Done():
							return
						case <-sub.ctx.Done():
							continue
						case <-time.After(backoff):
						}
					}

					stream, err := c.provider.CallStream(sub.ctx, sub.storeID, sub.initMsg)
					if err != nil {
						sub.failureCount.Add(1)
						sub.lastError.Store(err)
						if c.metrics != nil {
							c.metrics.RecordSubscriptionReplay(sub.id, false, time.Since(replayStart))
						}

						// Deliver nil so the goroutine re-enqueues itself
						select {
						case sub.streamCh <- nil:
						case <-sub.ctx.Done():
						}

						// Increase backoff for consecutive failures
						if backoff == 0 {
							backoff = 100 * time.Millisecond
						} else {
							backoff = applyBackoffJitter(backoff * 2)
							if backoff > maxBackoff {
								backoff = maxBackoff
							}
						}
						continue
					}

					// Success — reset backoff and deliver stream to the subscription goroutine
					backoff = 0
					if c.metrics != nil {
						c.metrics.RecordSubscriptionReplay(sub.id, true, time.Since(replayStart))
					}
					select {
					case sub.streamCh <- stream:
					case <-sub.ctx.Done():
						stream.Close(sub.ctx.Err())
					}
				}
			}
		}()
	})
}

// OnReconnected is called by the provider when it reconnects after a failure.
// Subscription goroutines that are waiting for a stream already have themselves
// enqueued in the reconnect queue. This callback is a no-op — the architecture
// ensures that disconnected subscriptions self-enqueue and the single-threaded
// dispatcher handles them serially.
// Implements api.ReconnectListener.
func (c *client) OnReconnected(ctx context.Context, storeID uuid.UUID) error {
	c.subscriptionsMu.RLock()
	count := len(c.subscriptions)
	c.subscriptionsMu.RUnlock()

	if count > 0 {
		slog.InfoContext(ctx, "client: provider reconnected, subscriptions will be restored serially", "count", count)
	}
	return nil
}

// Close gracefully shuts down the client, stopping the reconnection dispatcher
// and cleaning up resources. Safe to call multiple times.
func (c *client) Close() error {
	var closeErr error
	c.shutdownOnce.Do(func() {
		// Cancel dispatcher context to stop background goroutine
		if c.reconnectCancel != nil {
			c.reconnectCancel()
		}

		// Close reconnect queue after brief grace period for in-flight work
		time.Sleep(100 * time.Millisecond)
		close(c.reconnectQueue)

		// Unregister from provider
		c.provider.UnregisterReconnectListener(c)
	})
	return closeErr
}

// registerSubscription adds a subscription to the registry for replay on reconnect
func (c *client) registerSubscription(id string, sub *activeSubscription) {
	c.subscriptionsMu.Lock()
	defer c.subscriptionsMu.Unlock()
	c.subscriptions[id] = sub
}

// unregisterSubscription removes a subscription from the registry
func (c *client) unregisterSubscription(id string) {
	c.subscriptionsMu.Lock()
	defer c.subscriptionsMu.Unlock()
	delete(c.subscriptions, id)
}

// RemoveSubscription explicitly removes a subscription from the registry.
// Returns true if the subscription existed and was removed.
func (c *client) RemoveSubscription(id string) bool {
	c.subscriptionsMu.Lock()
	defer c.subscriptionsMu.Unlock()
	_, existed := c.subscriptions[id]
	delete(c.subscriptions, id)
	return existed
}

// RetryFailedSubscription requests that a subscription reconnect immediately.
// If the subscription is currently streaming, it will close the stream and re-enqueue
// for a new one on the next loop iteration. If it is already waiting in the reconnect
// queue or on a stream from the dispatcher, the next attempt is unchanged.
// This is useful when external conditions have changed (e.g., server restarted).
func (c *client) RetryFailedSubscription(id string) error {
	c.subscriptionsMu.RLock()
	sub, exists := c.subscriptions[id]
	c.subscriptionsMu.RUnlock()

	if !exists {
		return fmt.Errorf("subscription %s not found", id)
	}

	sub.retryRequested.Store(true)
	return nil
}

// SubscriptionStatus represents the health status of an active subscription.
type SubscriptionStatus struct {
	ID              string // subscription ID
	Status          string // "active" or "reconnecting"
	FailureCount    int32  // consecutive reconnection failures (resets on success)
	LastError       error  // last error encountered during reconnection
	LastSequence    int64  // last successfully delivered sequence
	HandlerTimeouts int32  // count of handler timeout occurrences
	HandlerPanics   int32  // count of handler panic occurrences
}

// GetSubscriptionStatus returns the current health status of a subscription.
//
// The returned status contains the subscription's current state: "active"
// (stream connected and delivering) or "reconnecting" (stream failed, retrying
// with exponential backoff). Subscriptions never permanently fail — they keep
// retrying until explicitly cancelled via Unsubscribe().
//
// Failure counts and last errors are tracked for observability. The failure
// count resets to zero on each successful reconnection. Returns nil if the
// subscription id is not found.
func (c *client) GetSubscriptionStatus(id string) *SubscriptionStatus {
	c.subscriptionsMu.RLock()
	sub, exists := c.subscriptions[id]
	c.subscriptionsMu.RUnlock()

	if !exists {
		return nil
	}

	status := "active"
	if s, ok := sub.status.Load().(string); ok {
		status = s
	}

	var lastErr error
	if e, ok := sub.lastError.Load().(error); ok {
		lastErr = e
	}

	return &SubscriptionStatus{
		ID:              sub.id,
		Status:          status,
		FailureCount:    sub.failureCount.Load(),
		LastError:       lastErr,
		LastSequence:    sub.lastDelivered.Load(),
		HandlerTimeouts: sub.handlerTimeouts.Load(),
		HandlerPanics:   sub.handlerPanics.Load(),
	}
}
