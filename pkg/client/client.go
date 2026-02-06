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
	c := &client{
		provider:       provider,
		policy:         policy,
		handlerTimeout: 30 * time.Second,
		subscriptions:  make(map[string]*activeSubscription),
		produceLocks:   make(map[string]*sync.Mutex),
	}
	provider.RegisterReconnectListener(c)
	return c
}

// NewClientWithHandlerTimeout creates a new Client with custom timeout for handler execution (Issue 7).
// Prevents slow handlers from blocking the subscription loop and starving other subscriptions.
// If a handler takes longer than the specified timeout, it is interrupted and execution continues
// with the next message. The timeout event is logged and tracked in subscription metrics.
func NewClientWithHandlerTimeout(provider api.BidiStreamProvider, handlerTimeout time.Duration) Client {
	c := &client{
		provider:       provider,
		policy:         DefaultRetryPolicy(),
		handlerTimeout: handlerTimeout,
		subscriptions:  make(map[string]*activeSubscription),
		produceLocks:   make(map[string]*sync.Mutex), // Issue 9: Initialize for Peek+Produce atomicity
	}

	// Register the client as a reconnect listener to replay subscriptions on reconnect
	provider.RegisterReconnectListener(c)

	return c
}

// activeSubscription represents a subscription that can be replayed on reconnection
type activeSubscription struct {
	id              string               // unique subscription ID
	storeID         uuid.UUID            // store being subscribed to
	initMsg         api.Routeable        // initial subscription message
	handler         func(*SegmentStatus) // handler to call for each status update
	cancel          context.CancelFunc   // cancels the subscription goroutine
	done            <-chan struct{}      // signals when subscription has stopped
	isReplaying     atomic.Bool          // prevents duplicate subscription during rapid reconnects
	lastDelivered   atomic.Int64         // tracks last delivered sequence for offset resumption
	ctx             context.Context      // subscription's own context (Issue 5)
	failureCount    atomic.Int32         // tracks consecutive replay failures (Issue 4)
	status          atomic.Value         // current status: "active"|"replaying"|"failed" (Issue 4)
	lastError       atomic.Value         // stores last error encountered (Issue 4)
	handlerTimeouts atomic.Int32         // counts handler timeout occurrences (Issue 7)
	handlerPanics   atomic.Int32         // counts handler panic occurrences
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
	ctx = context.WithValue(ctx, "request_id", requestID)
	slog.DebugContext(ctx, "consume space request",
		"request_id", requestID,
		"store_id", storeID,
		"space", args.Space)
	// Returns a resilience-wrapped enumerator that retries on disconnect (Issue 8)
	return newResilienceEnumerator(c, ctx, storeID, args)
}

func (c *client) ConsumeSegment(ctx context.Context, storeID uuid.UUID, args *api.ConsumeSegment) enumerators.Enumerator[*Entry] {
	requestID := uuid.New().String()
	ctx = context.WithValue(ctx, "request_id", requestID)
	slog.DebugContext(ctx, "consume segment request",
		"request_id", requestID,
		"store_id", storeID,
		"space", args.Space, "segment", args.Segment)
	// Returns a resilience-wrapped enumerator that retries on disconnect (Issue 8)
	return newResilienceEnumerator(c, ctx, storeID, args)
}

func (c *client) Consume(ctx context.Context, storeID uuid.UUID, args *api.Consume) enumerators.Enumerator[*Entry] {
	requestID := uuid.New().String()
	ctx = context.WithValue(ctx, "request_id", requestID)
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

	enumerator := api.NewStreamEnumerator[*SegmentStatus](bidi)
	// Consume statuses asynchronously so Publish doesn't block waiting for the server
	// to close the stream. This avoids hangs when the server sends a single status
	// and keeps the API simple for single-record publishes.
	go func() {
		_ = enumerators.Consume(enumerator)
	}()

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
		id:      subID,
		storeID: storeID,
		initMsg: initMsg,
		handler: handler,
		cancel:  cancel,
		done:    done,
		ctx:     ctx, // Store subscription's context (Issue 5)
	}

	// Initialize health status
	activeSub.status.Store("active")

	// Register subscription for replay on reconnect
	c.registerSubscription(subID, activeSub)

	go func() {
		defer close(done)
		defer func() {
			// Ensure cleanup happens even if handler panics. Do not unregister if subscription
			// has been marked as failed; leave it for explicit application control.
			if s, ok := activeSub.status.Load().(string); !ok || s != "failed" {
				c.unregisterSubscription(subID)
			}
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "subscription goroutine panic (cleanup completed)", "id", subID, "panic", r)
			}
		}()

		backoff := time.Second
		maxBackoff := 30 * time.Second
		attempt := 0

		for {
			// Check for cancellation before attempting (re)subscription
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Create new stream (initial or reconnect)
			attempt++
			stream, err := c.provider.CallStream(ctx, storeID, initMsg)
			if err != nil {
				slog.WarnContext(ctx, "subscription stream failed, retrying", "attempt", attempt, "backoff", backoff, "err", err)

				// If we've exhausted retry attempts for this batch, increment failure count
				if attempt >= 2 {
					failures := activeSub.failureCount.Add(1)
					if err != nil {
						activeSub.lastError.Store(err)
					}
					slog.WarnContext(ctx, "client: subscribe attempt limit reached",
						"id", subID, "consecutive_failures", failures, "err", err)

					if failures >= 2 {
						activeSub.status.Store("failed")
						slog.ErrorContext(ctx, "client: subscription marked as failed - max retry failures exceeded",
							"id", subID, "failures", failures, "err", err)
						// Keep subscription visible; stop trying
						return
					}

					// Reset attempt counter for next batch of retries
					attempt = 0
				}

				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
					// Exponential backoff with max and jitter
					backoff = backoff * 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
					backoff = applyBackoffJitter(backoff)
					continue
				}
			}

			// Reset backoff on successful connection
			backoff = time.Second
			slog.InfoContext(ctx, "subscription stream connected", "attempt", attempt)

			// Read from stream until it fails
			streamFailed := false
			for {
				select {
				case <-ctx.Done():
					stream.Close(ctx.Err())
					return
				default:
				}

				var status SegmentStatus
				if err := stream.Decode(&status); err != nil {
					slog.WarnContext(ctx, "subscription decode failed, reconnecting", "err", err)
					stream.Close(err)
					streamFailed = true
					break // inner loop - will reconnect with backoff
				}
				// Issue #1: Fixed goroutine leak by using context with timeout for handler
				// Do NOT spawn goroutine; execute handler with timeout inline using context
				func() {
					defer func() {
						if r := recover(); r != nil {
							activeSub.handlerPanics.Add(1)
							slog.ErrorContext(ctx, "subscription handler panic", "id", subID, "panic", r)
						}
					}()
					// Track last delivered sequence for offset tracking
					if status.LastSequence > 0 {
						activeSub.lastDelivered.Store(int64(status.LastSequence))
					}

					// Create timeout context for handler execution
					handlerCtx, cancel := context.WithTimeout(ctx, c.handlerTimeout)
					defer cancel()

					// Use a WaitGroup to block until handler completes or timeout
					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						defer func() {
							if r := recover(); r != nil {
								slog.ErrorContext(ctx, "subscription handler panic during execution", "id", subID, "panic", r)
							}
						}()
						handler(&status)
					}()

					// Wait for handler with timeout
					done := make(chan struct{})
					go func() {
						wg.Wait()
						close(done)
					}()

					select {
					case <-done:
						// Handler completed successfully
					case <-handlerCtx.Done():
						activeSub.handlerTimeouts.Add(1)
						slog.WarnContext(ctx, "subscription handler exceeded timeout", "id", subID, "timeout", c.handlerTimeout)
						// Note: We DO NOT forcibly kill the handler goroutine - it will continue
						// running in the background. Timeout is just logged. To prevent hangs,
						// the handler itself should check context deadlines.
					}
				}()
			}

			// Stream failed, apply backoff before reconnecting
			if streamFailed {
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
					backoff = backoff * 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				}
			}
			// Stream failed, loop will retry connection
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

// OnReconnected is called by the provider when it reconnects after a failure.
// It replays all active subscriptions to the new muxer connection.
// Implements api.ReconnectListener.
func (c *client) OnReconnected(ctx context.Context, storeID uuid.UUID) error {
	c.subscriptionsMu.RLock()
	defer c.subscriptionsMu.RUnlock()

	if len(c.subscriptions) == 0 {
		return nil
	}

	slog.InfoContext(ctx, "client: replaying subscriptions after reconnect", "count", len(c.subscriptions))

	// Replay each subscription in a separate goroutine to allow parallel recovery
	for _, sub := range c.subscriptions {
		// Skip subscriptions that are already marked as permanently failed
		if status, ok := sub.status.Load().(string); ok && status == "failed" {
			slog.DebugContext(ctx, "client: skipping permanently failed subscription", "id", sub.id)
			continue
		}

		// Only replay if not already replaying (prevents duplicate subscriptions on rapid reconnects)
		if !sub.isReplaying.CompareAndSwap(false, true) {
			continue // Already replaying, skip
		}

		go func(sub *activeSubscription) {
			defer func() {
				// Mark replay as complete when done (allows new replays if needed)
				sub.isReplaying.Store(false)
				if r := recover(); r != nil {
					slog.ErrorContext(sub.ctx, "replay goroutine panic", "id", sub.id, "panic", r)
				}
			}()
			slog.DebugContext(sub.ctx, "client: replaying subscription", "id", sub.id)
			// Re-establish the subscription using subscription's own context (Issue 5)
			// This ensures unsubscribe properly cancels replay goroutines
			err := c.replaySubscription(sub.ctx, sub)
			if err != nil {
				slog.WarnContext(sub.ctx, "client: subscription replay failed", "id", sub.id, "err", err)
			}
		}(sub)
	}

	return nil
}

// replaySubscription connects to a subscription and feeds status updates to the handler.
// This is used when reconnecting to re-establish a subscription after connection loss.
// Implements panic recovery to ensure failed handlers don't crash the replay.
// Issues 4, 5: Tracks failures and marks subscriptions as failed after threshold (Issue 4),
// uses subscription's context for proper lifecycle (Issue 5).
func (c *client) replaySubscription(ctx context.Context, sub *activeSubscription) error {
	const maxReplayAttempts = 2 // Limit retry attempts to prevent infinite loops (Issue 4)
	const maxFailures = 2       // After 2 consecutive failure batches, mark as failed (Issue 4)

	// Use exponential backoff for replay attempts
	backoff := time.Second
	maxBackoff := 30 * time.Second
	attempt := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		attempt++
		stream, err := c.provider.CallStream(ctx, sub.storeID, sub.initMsg)
		if err != nil {
			slog.WarnContext(ctx, "client: replay stream failed, retrying",
				"id", sub.id, "attempt", attempt, "backoff", backoff, "err", err)

			// If we've exhausted retry attempts, increment failure count (Issue 4)
			if attempt >= maxReplayAttempts {
				failures := sub.failureCount.Add(1)
				if err != nil {
					sub.lastError.Store(err)
				}
				slog.WarnContext(ctx, "client: replay attempt limit reached",
					"id", sub.id, "consecutive_failures", failures, "err", err)

				// After maxFailures consecutive failures, mark subscription as failed (Issue 4)
				if failures >= maxFailures {
					sub.status.Store("failed")
					slog.ErrorContext(ctx, "client: subscription marked as failed - max retry failures exceeded",
						"id", sub.id, "failures", failures, "err", err)
					c.unregisterSubscription(sub.id) // Remove from registry (Issue 6)
					return fmt.Errorf("subscription failed after %d consecutive reconnect failures: %w", failures, err)
				}

				// Reset attempt counter for next batch of retries
				attempt = 0
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff = backoff * 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
		}

		// Successfully connected, reset failure count and deliver updates (Issue 4)
		sub.failureCount.Store(0)
		sub.status.Store("active") // Reset to active on successful connection (Issue 4)

		backoff = time.Second
		slog.DebugContext(ctx, "client: replay stream connected", "id", sub.id)

		streamFailed := false
		for {
			var status SegmentStatus
			if err := stream.Decode(&status); err != nil {
				slog.DebugContext(ctx, "client: replay stream decode failed", "id", sub.id, "err", err)
				stream.Close(err)
				streamFailed = true
				break // inner loop, will retry with backoff
			}
			// Call handler with panic recovery to prevent replay crashes
			func() {
				defer func() {
					if r := recover(); r != nil {
						slog.ErrorContext(ctx, "subscription handler panic during replay", "id", sub.id, "panic", r)
					}
				}()
				// Track last delivered sequence for potential offset resumption
				if status.LastSequence > 0 {
					sub.lastDelivered.Store(int64(status.LastSequence))
				}
				sub.handler(&status)
			}()
		}

		// Stream failed, apply backoff before retrying
		if streamFailed {
			sub.status.Store("replaying") // Mark as replaying (Issue 4)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff = backoff * 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
		}
	}
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

// RetryFailedSubscription triggers a manual retry of a subscription previously
// marked as failed. Returns an error if the subscription does not exist or
// is not in the "failed" state.
func (c *client) RetryFailedSubscription(id string) error {
	c.subscriptionsMu.RLock()
	sub, exists := c.subscriptions[id]
	c.subscriptionsMu.RUnlock()

	if !exists {
		return fmt.Errorf("subscription %s not found", id)
	}

	status, _ := sub.status.Load().(string)
	if status != "failed" {
		return fmt.Errorf("subscription %s is not failed (status: %s)", id, status)
	}

	// Reset failure state
	sub.failureCount.Store(0)
	sub.status.Store("replaying")
	// atomic.Value does not accept nil in Store; use empty error placeholder
	sub.lastError.Store(errors.New(""))

	// Trigger replay in background
	go func() {
		if err := c.replaySubscription(sub.ctx, sub); err != nil {
			slog.ErrorContext(sub.ctx, "manual subscription retry failed", "id", id, "err", err)
		}
	}()

	return nil
}

// SubscriptionStatus represents the health status of an active subscription (Issue 4)
type SubscriptionStatus struct {
	ID              string // subscription ID
	Status          string // "active", "replaying", or "failed"
	FailureCount    int32  // consecutive replay failures
	LastError       error  // last error encountered during replay
	LastSequence    int64  // last successfully delivered sequence
	HandlerTimeouts int32  // count of handler timeout occurrences (Issue 7)
	HandlerPanics   int32  // count of handler panic occurrences
}

// GetSubscriptionStatus returns the current health status of a subscription.
//
// The returned status contains the subscription's current state: "active",
// "replaying", or "failed". Failed subscriptions are intentionally kept in the
// registry so that applications can inspect them, retry via `RetryFailedSubscription`,
// or remove them explicitly with `RemoveSubscription`.
//
// Failure counts and last errors are tracked to help with observability and
// operational recovery. Returns nil if the subscription id is not found.
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
