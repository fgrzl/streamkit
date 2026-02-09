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
	"github.com/google/uuid"
)

// Issue #24: Use typed context keys instead of strings to avoid collisions
type contextKey string

const (
	requestIDKey contextKey = "request_id" // Typed key to prevent collisions with other packages
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
		provider:        provider,
		policy:          policy,
		handlerTimeout:  30 * time.Second,
		subscriptions:   make(map[string]*activeSubscription),
		reconnectQueue:  make(chan []string, 100), // large buffer to prevent signal loss
		reconnectCtx:    ctx,
		reconnectCancel: cancel,
		produceLocks:    make(map[string]*sync.Mutex),
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
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		provider:        provider,
		policy:          DefaultRetryPolicy(),
		handlerTimeout:  handlerTimeout,
		subscriptions:   make(map[string]*activeSubscription),
		reconnectQueue:  make(chan []string, 100), // large buffer to prevent signal loss
		reconnectCtx:    ctx,
		reconnectCancel: cancel,
		produceLocks:    make(map[string]*sync.Mutex), // Issue 9: Initialize for Peek+Produce atomicity
	}

	c.startReconnectDispatcher()

	// Register the client as a reconnect listener to replay subscriptions on reconnect
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
	activeHandlers  atomic.Int32         // tracks currently running handlers to prevent goroutine leak
	reconnectSignal chan struct{}        // signaled by OnReconnected to wake subscription from backoff
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
	provider       api.BidiStreamProvider
	policy         RetryPolicy
	handlerTimeout time.Duration // max time allowed for handler to execute (Issue 7)

	// subscription registry for tracking active subscriptions
	subscriptionsMu sync.RWMutex
	subscriptions   map[string]*activeSubscription

	// Single-threaded reconnection dispatcher to prevent thundering herd
	reconnectQueue  chan []string   // queue of subscription ID batches to reconnect
	reconnectLoop   sync.Once       // ensures reconnect dispatcher starts only once
	reconnectCtx    context.Context // context for dispatcher lifecycle
	reconnectCancel context.CancelFunc
	shutdownOnce    sync.Once // ensures clean shutdown

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
	requestID := uuid.New().String()
	ctx = context.WithValue(ctx, requestIDKey, requestID)
	slog.DebugContext(ctx, "consume space request",
		"request_id", requestID,
		"store_id", storeID,
		"space", args.Space)
	// Returns a resilience-wrapped enumerator that retries on disconnect (Issue 8)
	return newResilienceEnumerator(c, ctx, storeID, args)
}

func (c *client) ConsumeSegment(ctx context.Context, storeID uuid.UUID, args *api.ConsumeSegment) enumerators.Enumerator[*Entry] {
	requestID := uuid.New().String()
	ctx = context.WithValue(ctx, requestIDKey, requestID)
	slog.DebugContext(ctx, "consume segment request",
		"request_id", requestID,
		"store_id", storeID,
		"space", args.Space, "segment", args.Segment)
	// Returns a resilience-wrapped enumerator that retries on disconnect (Issue 8)
	return newResilienceEnumerator(c, ctx, storeID, args)
}

func (c *client) Consume(ctx context.Context, storeID uuid.UUID, args *api.Consume) enumerators.Enumerator[*Entry] {
	requestID := uuid.New().String()
	ctx = context.WithValue(ctx, requestIDKey, requestID)
	slog.DebugContext(ctx, "consume request",
		"request_id", requestID,
		"store_id", storeID)
	// Returns a resilience-wrapped enumerator that retries on disconnect (Issue 8)
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
				// Update the consume args with new position
				e.updateConsumePosition()
				e.attemptCount++
				e.disconnectedAt = time.Now()
				slog.InfoContext(e.ctx, "resilience enumerator: retrying from last position", "sequence", e.lastEntry.Sequence, "attempt", e.attemptCount, "err", streamErr)
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
			slog.DebugContext(e.ctx, "resilience: updated ConsumeSegment offset",
				"space", args.Space, "segment", args.Segment, "minSeq", args.MinSequence)
		}
	case *api.ConsumeSpace:
		// For ConsumeSpace, we need to resume past the last timestamp
		// Add 1 nanosecond to avoid re-receiving entries with same timestamp
		if e.lastEntry != nil && e.lastEntry.Timestamp > 0 {
			args.MinTimestamp = e.lastEntry.Timestamp + 1
			slog.DebugContext(e.ctx, "resilience: updated ConsumeSpace offset",
				"space", args.Space, "minTimestamp", args.MinTimestamp)
		}
	case *api.Consume:
		// For Consume (multi-space), update offset map with last seen position
		// If Offsets map exists, update the specific space/segment pair
		if e.lastEntry != nil && args.Offsets != nil {
			// Create offset key for this space/segment
			offsetKey := e.lastEntry.Space + "/" + e.lastEntry.Segment
			// Store sequence after last consumed entry using lexkey encoding
			args.Offsets[offsetKey] = lexkey.Encode(e.lastEntry.Sequence + 1)
			slog.DebugContext(e.ctx, "resilience: updated Consume offset",
				"offsetKey", offsetKey, "sequence", e.lastEntry.Sequence+1)
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
		slog.DebugContext(e.ctx, "resilience: built updated ConsumeSegment args",
			"space", newArgs.Space, "segment", newArgs.Segment, "minSeq", newArgs.MinSequence)
		return &newArgs
	case *api.ConsumeSpace:
		newArgs := *args
		newArgs.Offset = e.lastEntry.GetSpaceOffset()
		slog.DebugContext(e.ctx, "resilience: built updated ConsumeSpace args",
			"space", newArgs.Space, "offset", newArgs.Offset)
		return &newArgs
	case *api.Consume:
		newArgs := *args
		if newArgs.Offsets == nil {
			newArgs.Offsets = make(map[string]lexkey.LexKey)
		}
		offsetKey := e.lastEntry.Space + "/" + e.lastEntry.Segment
		newArgs.Offsets[offsetKey] = lexkey.Encode(e.lastEntry.Timestamp, e.lastEntry.Space, e.lastEntry.Segment, e.lastEntry.Sequence)
		slog.DebugContext(e.ctx, "resilience: built updated Consume args",
			"offsetKey", offsetKey, "offset", newArgs.Offsets[offsetKey])
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
	// Debug: log returned peek entry
	slog.InfoContext(ctx, "peek: returned entry", "space", space, "segment", segment, "seq", entry.Sequence)
	return entry, nil
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

	// Evict old locks if map is too large (simple random eviction)
	if len(c.produceLocks) >= MaxProduceLocks {
		toRemove := MaxProduceLocks / 10
		for k := range c.produceLocks {
			delete(c.produceLocks, k)
			toRemove--
			if toRemove <= 0 {
				break
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
	// Debug: log peeked sequence for diagnostics
	slog.InfoContext(ctx, "publish: peek sequence", "space", space, "segment", segment, "seq", peek.Sequence)
	// If peek returns sequence 0, retry a few times to handle transient peek anomalies.
	for i := 0; i < 3 && peek.Sequence == 0; i++ {
		// Issue #2: Check context before sleeping
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
			// Verify context still not canceled after sleep
			if err := ctx.Err(); err != nil {
				return err
			}
			peek, err = c.Peek(ctx, storeID, space, segment)
			if err != nil {
				return err
			}
			slog.InfoContext(ctx, "publish: retry peek sequence", "space", space, "segment", segment, "seq", peek.Sequence, "attempt", i+1)
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
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	// Generate unique subscription ID for tracking
	subID := uuid.New().String()

	// Create the activeSubscription for replaying on reconnect
	activeSub := &activeSubscription{
		id:              subID,
		storeID:         storeID,
		initMsg:         initMsg,
		handler:         handler,
		cancel:          cancel,
		done:            done,
		ctx:             ctx,
		reconnectSignal: make(chan struct{}, 1),
	}

	// Initialize health status
	activeSub.status.Store("active")

	// Register subscription for replay on reconnect
	c.registerSubscription(subID, activeSub)

	go func() {
		defer close(done)
		defer c.unregisterSubscription(subID)
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "subscription goroutine panic (cleanup completed)", "id", subID, "panic", r)
			}
		}()

		backoff := time.Second
		maxBackoff := 30 * time.Second

		for {
			// Check for cancellation before attempting (re)subscription
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Create new stream (initial or reconnect)
			stream, err := c.provider.CallStream(ctx, storeID, initMsg)
			if err != nil {
				activeSub.failureCount.Add(1)
				activeSub.lastError.Store(err)
				activeSub.status.Store("reconnecting")
				slog.WarnContext(ctx, "subscription stream failed, retrying",
					"id", subID, "backoff", backoff, "failures", activeSub.failureCount.Load(), "err", err)

				// Wait with backoff, but wake immediately on provider reconnect signal
				select {
				case <-ctx.Done():
					return
				case <-activeSub.reconnectSignal:
					// Provider reconnected — retry immediately with reset backoff
					backoff = time.Second
				case <-time.After(applyBackoffJitter(backoff)):
					backoff = backoff * 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				}
				continue
			}

			// Connected successfully — reset failure state
			activeSub.failureCount.Store(0)
			activeSub.status.Store("active")
			backoff = time.Second
			slog.InfoContext(ctx, "subscription stream connected", "id", subID)

			// Drain any stale reconnect signals from before we connected
			select {
			case <-activeSub.reconnectSignal:
			default:
			}

			// Read from stream until it fails or context is cancelled
			for {
				select {
				case <-ctx.Done():
					stream.Close(ctx.Err())
					return
				default:
				}

				var status SegmentStatus
				if err := stream.Decode(&status); err != nil {
					activeSub.failureCount.Add(1)
					activeSub.lastError.Store(err)
					activeSub.status.Store("reconnecting")
					slog.WarnContext(ctx, "subscription decode failed, reconnecting",
						"id", subID, "err", err)
					stream.Close(err)
					break // inner loop → reconnect with backoff
				}

				// Reset failure count on successful decode
				if activeSub.failureCount.Load() > 0 {
					activeSub.failureCount.Store(0)
				}

				// Limit concurrent handler goroutines to prevent unbounded accumulation
				const maxActiveHandlers = 10
				if activeSub.activeHandlers.Load() >= maxActiveHandlers {
					slog.WarnContext(ctx, "subscription: too many active handlers, skipping message",
						"id", subID, "activeHandlers", activeSub.activeHandlers.Load())
					continue
				}

				activeSub.activeHandlers.Add(1)
				go func() {
					defer activeSub.activeHandlers.Add(-1)

					// Track last delivered sequence for offset tracking
					if status.LastSequence > 0 {
						activeSub.lastDelivered.Store(int64(status.LastSequence))
					}

					// Create timeout context for handler execution
					handlerCtx, cancel := context.WithTimeout(ctx, c.handlerTimeout)
					defer cancel()

					// Run handler in a goroutine so we can enforce the timeout.
					// The recover() must be inside this goroutine since panics
					// don't propagate across goroutine boundaries.
					done := make(chan struct{})
					go func() {
						defer close(done)
						defer func() {
							if r := recover(); r != nil {
								activeSub.handlerPanics.Add(1)
								slog.ErrorContext(ctx, "subscription handler panic", "id", subID, "panic", r)
							}
						}()
						handler(&status)
					}()

					select {
					case <-done:
						// Handler completed (or panicked and recovered)
					case <-handlerCtx.Done():
						activeSub.handlerTimeouts.Add(1)
						slog.WarnContext(ctx, "subscription handler exceeded timeout", "id", subID, "timeout", c.handlerTimeout)
					}
				}()
			}

			// Stream failed — backoff before reconnecting, but wake on provider reconnect
			select {
			case <-ctx.Done():
				return
			case <-activeSub.reconnectSignal:
				// Provider reconnected — retry immediately with reset backoff
				backoff = time.Second
			case <-time.After(applyBackoffJitter(backoff)):
				backoff = backoff * 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
		}
	}()

	return &subscription{cancel: cancel, done: done}, nil
}

type subscription struct {
	cancel func()
	done   <-chan struct{}
}

func (s *subscription) Unsubscribe() {
	s.cancel()
	<-s.done
}

// startReconnectDispatcher starts a single-threaded goroutine that serially
// processes reconnection requests. This prevents thundering herd when many
// subscriptions need to reconnect simultaneously after provider reconnection.
func (c *client) startReconnectDispatcher() {
	c.reconnectLoop.Do(func() {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("reconnect dispatcher panic", "err", r)
				}
			}()

			for {
				select {
				case <-c.reconnectCtx.Done():
					slog.Debug("reconnect dispatcher: shutting down")
					return
				case subIDs, ok := <-c.reconnectQueue:
					// Channel closed - exit gracefully
					if !ok {
						slog.Debug("reconnect dispatcher: queue closed, exiting")
						return
					}

					if len(subIDs) == 0 {
						continue
					}

					slog.Debug("reconnect dispatcher: processing batch", "count", len(subIDs))

					// Collect subscription pointers once under lock to avoid repeated lock churn
					c.subscriptionsMu.RLock()
					subs := make([]*activeSubscription, 0, len(subIDs))
					for _, id := range subIDs {
						if sub, exists := c.subscriptions[id]; exists {
							subs = append(subs, sub)
						}
					}
					c.subscriptionsMu.RUnlock()

					// Process batch with size limit to prevent blocking dispatcher too long
					const maxBatchSize = 50
					for len(subs) > 0 {
						batchSize := len(subs)
						if batchSize > maxBatchSize {
							batchSize = maxBatchSize
						}
						batch := subs[:batchSize]
						subs = subs[batchSize:]

						// Signal each subscription in batch with reduced stagger (10ms)
						for i, sub := range batch {
							// Check for shutdown between each signal
							select {
							case <-c.reconnectCtx.Done():
								slog.Debug("reconnect dispatcher: shutdown during batch")
								return
							default:
							}

							// Stagger reconnections to avoid overwhelming the connection
							if i > 0 {
								// Interruptible 10ms stagger (reduced from 50ms)
								select {
								case <-c.reconnectCtx.Done():
									return
								case <-time.After(10 * time.Millisecond):
								}
							}

							// Non-blocking send to subscription's reconnect signal
							select {
							case sub.reconnectSignal <- struct{}{}:
								slog.Debug("reconnect dispatcher: signaled subscription", "id", sub.id)
							default:
								// Buffer full - subscription already has pending reconnect signal
								slog.Debug("reconnect dispatcher: subscription already signaled", "id", sub.id)
							}
						}

						// Brief pause between batches if more remain
						if len(subs) > 0 {
							select {
							case <-c.reconnectCtx.Done():
								return
							case <-time.After(50 * time.Millisecond):
							}
						}
					}

					slog.Debug("reconnect dispatcher: batch complete")
				}
			}
		}()
	})
}

// OnReconnected is called by the provider when it reconnects after a failure.
// It enqueues subscription IDs to be processed by the single-threaded reconnect
// dispatcher, which staggers the reconnection signals to prevent thundering herd.
// Implements api.ReconnectListener.
func (c *client) OnReconnected(ctx context.Context, storeID uuid.UUID) error {
	c.subscriptionsMu.RLock()
	if len(c.subscriptions) == 0 {
		c.subscriptionsMu.RUnlock()
		return nil
	}

	// Collect all subscription IDs
	subIDs := make([]string, 0, len(c.subscriptions))
	for id := range c.subscriptions {
		subIDs = append(subIDs, id)
	}
	c.subscriptionsMu.RUnlock()

	slog.InfoContext(ctx, "client: enqueuing subscriptions for reconnection", "count", len(subIDs))

	// Non-blocking send to dispatcher queue
	select {
	case c.reconnectQueue <- subIDs:
		slog.Debug("client: reconnection batch enqueued")
	default:
		// Queue full - likely already have pending reconnection work
		slog.Warn("client: reconnect queue full, skipping duplicate signal")
	}

	return nil
}

// Close gracefully shuts down the client, stopping the reconnection dispatcher
// and cleaning up resources. Safe to call multiple times.
func (c *client) Close() error {
	var closeErr error
	c.shutdownOnce.Do(func() {
		slog.Debug("client: shutting down")

		// Cancel dispatcher context to stop background goroutine
		if c.reconnectCancel != nil {
			c.reconnectCancel()
		}

		// Close reconnect queue after brief grace period for in-flight work
		time.Sleep(100 * time.Millisecond)
		close(c.reconnectQueue)

		// Unregister from provider
		c.provider.UnregisterReconnectListener(c)

		slog.Debug("client: shutdown complete")
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

// RetryFailedSubscription signals a subscription to immediately retry its
// stream connection. This is useful when external conditions have changed
// (e.g., server restarted) and the caller wants to bypass the backoff wait.
func (c *client) RetryFailedSubscription(id string) error {
	c.subscriptionsMu.RLock()
	sub, exists := c.subscriptions[id]
	c.subscriptionsMu.RUnlock()

	if !exists {
		return fmt.Errorf("subscription %s not found", id)
	}

	// Signal the subscription goroutine to wake from backoff and retry immediately
	select {
	case sub.reconnectSignal <- struct{}{}:
	default:
		// Already signaled
	}

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
