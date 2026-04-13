package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgrzl/lexkey"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/transport/wskit"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockBidiStream is a test double for api.BidiStream
type mockBidiStream struct {
	mu          sync.Mutex
	encoded     []interface{}
	encodeFn    func(m any) error
	decodeFn    func(m any) error
	closeFn     func(error)
	closeSendFn func(error) error
	closedChan  chan struct{}
}

func (m *mockBidiStream) Encode(msg any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.encodeFn != nil {
		return m.encodeFn(msg)
	}
	m.encoded = append(m.encoded, msg)
	return nil
}

func (m *mockBidiStream) Decode(msg any) error {
	if m.decodeFn != nil {
		return m.decodeFn(msg)
	}
	return nil
}

func (m *mockBidiStream) CloseSend(err error) error {
	if m.closeSendFn != nil {
		return m.closeSendFn(err)
	}
	return nil
}

func (m *mockBidiStream) Close(err error) {
	if m.closeFn != nil {
		m.closeFn(err)
	}
	// idempotent close - mock only closes once
	select {
	case <-m.closedChan:
		// already closed
	default:
		close(m.closedChan)
	}
}

func (m *mockBidiStream) EndOfStreamError() error {
	return errors.New("end of stream")
}

func (m *mockBidiStream) Closed() <-chan struct{} {
	return m.closedChan
}

var _ api.BidiStream = (*mockBidiStream)(nil)

// mockProvider is a test double for api.BidiStreamProvider
type mockProvider struct {
	mu                sync.RWMutex
	callStreamFn      func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error)
	listeners         []api.ReconnectListener
	closedStreamsFunc func(ctx context.Context)
}

func (m *mockProvider) CallStream(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
	if m.callStreamFn != nil {
		return m.callStreamFn(ctx, storeID, routeable)
	}
	return &mockBidiStream{closedChan: make(chan struct{})}, nil
}

func (m *mockProvider) RegisterReconnectListener(listener api.ReconnectListener) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listeners = append(m.listeners, listener)
}

func (m *mockProvider) UnregisterReconnectListener(listener api.ReconnectListener) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, l := range m.listeners {
		if l == listener {
			m.listeners = append(m.listeners[:i], m.listeners[i+1:]...)
			break
		}
	}
}

func (m *mockProvider) GetListeners() []api.ReconnectListener {
	m.mu.RLock()
	defer m.mu.RUnlock()
	listeners := make([]api.ReconnectListener, len(m.listeners))
	copy(listeners, m.listeners)
	return listeners
}

var _ api.BidiStreamProvider = (*mockProvider)(nil)

type testClientMetrics struct {
	handlerTimeouts    atomic.Int32
	handlerPanics      atomic.Int32
	reconnectDepth     atomic.Int64
	reconnectMaxDepth  atomic.Int64
	reconnectBlocked   atomic.Int32
	reconnectWaitCount atomic.Int32
	reconnectMaxWaitNs atomic.Int64
}

func (m *testClientMetrics) RecordProduceLatency(space, segment string, duration time.Duration) {}

func (m *testClientMetrics) RecordConsumeLatency(space, segment string, duration time.Duration) {}

func (m *testClientMetrics) RecordSubscriptionReplay(id string, success bool, duration time.Duration) {
}

func (m *testClientMetrics) RecordHandlerTimeout(id string) {
	m.handlerTimeouts.Add(1)
}

func (m *testClientMetrics) RecordHandlerPanic(id string) {
	m.handlerPanics.Add(1)
}

func (m *testClientMetrics) RecordReconnectQueueDepth(depth int) {
	value := int64(depth)
	m.reconnectDepth.Store(value)
	for {
		current := m.reconnectMaxDepth.Load()
		if value <= current || m.reconnectMaxDepth.CompareAndSwap(current, value) {
			return
		}
	}
}

func (m *testClientMetrics) RecordReconnectQueueBlocked(duration time.Duration) {
	m.reconnectBlocked.Add(1)
	m.RecordReconnectQueueWait(duration)
}

func (m *testClientMetrics) RecordReconnectQueueWait(duration time.Duration) {
	m.reconnectWaitCount.Add(1)
	value := duration.Nanoseconds()
	for {
		current := m.reconnectMaxWaitNs.Load()
		if value <= current || m.reconnectMaxWaitNs.CompareAndSwap(current, value) {
			return
		}
	}
}

func withSubscriptionHeartbeatTiming(interval, timeout time.Duration, fn func()) {
	oldInterval := subscriptionHeartbeatInterval
	oldTimeout := subscriptionHeartbeatTimeout
	subscriptionHeartbeatInterval = interval
	subscriptionHeartbeatTimeout = timeout
	defer func() {
		subscriptionHeartbeatInterval = oldInterval
		subscriptionHeartbeatTimeout = oldTimeout
	}()
	fn()
}

func TestShouldClientDoesNotRegisterReconnectListenerOnCreation(t *testing.T) {
	provider := &mockProvider{}
	c := NewClient(provider)
	require.NotNil(t, c)

	// Assert: the client no longer relies on provider reconnect notifications.
	listeners := provider.GetListeners()
	assert.Empty(t, listeners)
}

func TestShouldSubscriptionTrackedInRegistry(t *testing.T) {
	// Arrange
	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			emitted := false
			stream.decodeFn = func(m any) error {
				if !emitted {
					emitted = true
					if status, ok := m.(*SegmentStatus); ok {
						*status = SegmentStatus{Segment: "test-segment"}
					}
					return nil
				}
				if status, ok := m.(*SegmentStatus); ok {
					*status = SegmentStatus{Segment: "test-segment"}
				}
				<-ctx.Done()
				return ctx.Err()
			}

			return stream, nil
		},
	}
	c := NewClient(provider)

	// Act: create a subscription
	ctx := context.Background()
	storeID := uuid.New()
	space := "test-space"
	segment := "test-segment"

	updateCount := 0
	handler := func(status *SegmentStatus) {
		updateCount++
	}

	sub, err := c.SubscribeToSegment(ctx, storeID, space, segment, handler)
	require.NoError(t, err)
	require.NotNil(t, sub)

	// Give the subscription goroutine time to process
	time.Sleep(100 * time.Millisecond)

	// Assert: subscription should be tracked in registry
	clientInst := c.(*client)
	activeSubs := clientInst.subscriptionCount()

	// Subscription should be active (at least initially before stream ends)
	assert.Greater(t, activeSubs, 0)

	// Cleanup
	sub.Unsubscribe()

	// Give unsubscribe time to process
	time.Sleep(100 * time.Millisecond)

	// Assert: subscription should be removed from registry after unsubscribe
	activeSubs = clientInst.subscriptionCount()
	assert.Equal(t, 0, activeSubs)
}

func TestShouldSubscriptionStopsAfterStreamDisconnect(t *testing.T) {
	var streamAttempts atomic.Int32
	updates := make(chan uint64, 2)
	firstStreamReady := make(chan *mockBidiStream, 1)

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			streamAttempts.Add(1)
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			emitted := false
			stream.decodeFn = func(m any) error {
				if !emitted {
					emitted = true
					status, ok := m.(*SegmentStatus)
					if !ok {
						return errors.New("unexpected decode target")
					}
					*status = SegmentStatus{Space: "space", Segment: "segment", LastSequence: 1}
					return nil
				}
				<-stream.closedChan
				return errors.New("connection lost")
			}
			select {
			case firstStreamReady <- stream:
			default:
			}
			return stream, nil
		},
	}

	c := NewClient(provider)
	ctx := context.Background()

	sub, err := c.SubscribeToSegment(ctx, uuid.New(), "space", "segment", func(status *SegmentStatus) {
		select {
		case updates <- status.LastSequence:
		default:
		}
	})
	require.NoError(t, err)

	select {
	case seq := <-updates:
		assert.Equal(t, uint64(1), seq)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for initial subscription update")
	}

	var firstStream *mockBidiStream
	select {
	case firstStream = <-firstStreamReady:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for initial stream instance")
	}
	firstStream.Close(errors.New("connection lost"))

	require.Eventually(t, func() bool {
		return c.GetSubscriptionStatus(sub.ID()) == nil
	}, time.Second, 10*time.Millisecond, "subscription should terminate after the stream disconnects")
	assert.Equal(t, int32(1), streamAttempts.Load(), "subscriptions should not reopen a new stream after disconnect")

	sub.Unsubscribe()
}

func TestShouldOnReconnectedDoesNotRestoreStoppedSubscriptions(t *testing.T) {
	var streamAttempts atomic.Int32
	firstStreamReady := make(chan *mockBidiStream, 1)

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			streamAttempts.Add(1)
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
				<-stream.closedChan
				return errors.New("connection lost")
			}
			select {
			case firstStreamReady <- stream:
			default:
			}
			return stream, nil
		},
	}

	c := NewClient(provider).(*client)
	sub, err := c.SubscribeToSegment(context.Background(), uuid.New(), "space", "segment", func(*SegmentStatus) {})
	require.NoError(t, err)

	var firstStream *mockBidiStream
	select {
	case firstStream = <-firstStreamReady:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for initial stream instance")
	}
	firstStream.Close(errors.New("connection lost"))

	require.Eventually(t, func() bool {
		return c.GetSubscriptionStatus(sub.ID()) == nil
	}, time.Second, 10*time.Millisecond)

	err = c.OnReconnected(context.Background(), uuid.Nil)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), streamAttempts.Load(), "provider reconnect notifications must not recreate stopped logical streams")

	sub.Unsubscribe()
}

func TestShouldSubscriptionFiltersHeartbeats(t *testing.T) {
	var decodeCount atomic.Int32
	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
				status, ok := m.(*SegmentStatus)
				if !ok {
					return errors.New("unexpected decode target")
				}

				switch decodeCount.Add(1) {
				case 1:
					*status = SegmentStatus{Space: "space", Segment: "segment", Heartbeat: true}
					return nil
				case 2:
					*status = SegmentStatus{Space: "space", Segment: "segment", LastSequence: 42}
					return nil
				default:
					<-ctx.Done()
					return ctx.Err()
				}
			}
			return stream, nil
		},
	}

	c := NewClient(provider)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updates := make(chan SegmentStatus, 1)
	sub, err := c.SubscribeToSegment(ctx, uuid.New(), "space", "segment", func(status *SegmentStatus) {
		updates <- *status
		cancel()
	})
	require.NoError(t, err)

	select {
	case status := <-updates:
		assert.False(t, status.Heartbeat)
		assert.Equal(t, uint64(42), status.LastSequence)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for non-heartbeat update")
	}

	sub.Unsubscribe()
}

func TestShouldSubscriptionDoesNotRequestHeartbeatsByDefault(t *testing.T) {
	requested := make(chan *api.SubscribeToSegmentStatus, 1)
	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			args, ok := routeable.(*api.SubscribeToSegmentStatus)
			require.True(t, ok, "expected subscription request")
			select {
			case requested <- args:
			default:
			}

			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
				<-ctx.Done()
				return ctx.Err()
			}
			return stream, nil
		},
	}

	c := NewClient(provider).(*client)
	sub, err := c.SubscribeToSegment(context.Background(), uuid.New(), "space", "segment", func(*SegmentStatus) {})
	require.NoError(t, err)

	var args *api.SubscribeToSegmentStatus
	select {
	case args = <-requested:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for subscription request")
	}
	require.NotNil(t, args)
	assert.Equal(t, int64(0), args.HeartbeatIntervalSeconds)

	time.Sleep(150 * time.Millisecond)
	assert.NotNil(t, c.GetSubscriptionStatus(sub.ID()), "idle subscriptions should stay alive without default heartbeat timeouts")

	sub.Unsubscribe()
	assert.Nil(t, c.GetSubscriptionStatus(sub.ID()))
}

func TestShouldSubscriptionStopsWhenHeartbeatsStop(t *testing.T) {
	withSubscriptionHeartbeatTiming(25*time.Millisecond, 80*time.Millisecond, func() {
		var attempts atomic.Int32
		provider := &mockProvider{
			callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
				attempts.Add(1)
				stream := &mockBidiStream{closedChan: make(chan struct{})}
				stream.decodeFn = func(m any) error {
					<-stream.closedChan
					return errors.New("stream closed")
				}
				return stream, nil
			},
		}

		c := NewClient(provider).(*client)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sub, err := c.SubscribeToSegment(ctx, uuid.New(), "space", "segment", func(status *SegmentStatus) {})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			return c.GetSubscriptionStatus(sub.ID()) == nil
		}, time.Second, 10*time.Millisecond, "subscription should terminate after heartbeat timeout")
		assert.Equal(t, int32(1), attempts.Load(), "subscription should not reconnect after heartbeat timeout")
		sub.Unsubscribe()
	})
}

func TestShouldHandlerPanicResilience(t *testing.T) {
	// Arrange
	// This tests Critical Issue #1: Handler Panics Crash Subscriptions
	// We ensure that a panicking handler doesn't crash the subscription goroutine
	// and that the subscription continues to function.

	messagesSent := 0
	messagesSentMu := sync.Mutex{}

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}

			stream.decodeFn = func(m any) error {
				messagesSentMu.Lock()
				messagesSent++
				currentMsg := messagesSent
				messagesSentMu.Unlock()

				if status, ok := m.(*SegmentStatus); ok {
					status.Segment = "test-segment"
					status.LastSequence = uint64(currentMsg)
				}

				// After first message, cancel to end stream
				if currentMsg >= 3 {
					<-ctx.Done()
					return ctx.Err()
				}

				return nil
			}

			return stream, nil
		},
	}

	c := NewClient(provider)
	ctx, cancel := context.WithCancel(context.Background())
	storeID := uuid.New()

	panicCount := 0
	panicCountMu := sync.Mutex{}
	successCount := 0
	successCountMu := sync.Mutex{}

	// Handler that panics on first call, then works normally
	handler := func(status *SegmentStatus) {
		if status.LastSequence == 1 {
			panicCountMu.Lock()
			panicCount++
			panicCountMu.Unlock()
			panic("intentional panic for testing")
		}
		successCountMu.Lock()
		successCount++
		successCountMu.Unlock()
	}

	// Act: create subscription with panicking handler
	sub, err := c.SubscribeToSegment(ctx, storeID, "space", "segment", handler)
	require.NoError(t, err)
	require.NotNil(t, sub)

	// Give subscription time to process messages
	time.Sleep(500 * time.Millisecond)

	// Assert: subscription should still work even after panic
	// We should have experienced one panic but the subscription should continue
	panicCountMu.Lock()
	panicOccurrences := panicCount
	panicCountMu.Unlock()

	successCountMu.Lock()
	successOccurrences := successCount
	successCountMu.Unlock()

	// The first message causes panic, but subsequent messages should still be delivered
	assert.Equal(t, 1, panicOccurrences, "should have one panic")
	assert.Greater(t, successOccurrences, 0, "subsequent messages should still be delivered after panic")

	// Verify the subscription is still in registry and functional
	clientInst := c.(*client)
	activeSubs := clientInst.subscriptionCount()
	assert.Greater(t, activeSubs, 0, "subscription should still be active after handler panic")

	// Cleanup
	cancel()
	sub.Unsubscribe()
}

// TestHealthStatusTracking tests Issue 4: Silent subscription failures detection
func TestShouldHealthStatusTracking(t *testing.T) {
	// Arrange
	// This tests Issue #4: Silent Subscription Failures
	// Verify that SubscriptionStatus helper can check health of active subscriptions

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
				if status, ok := m.(*SegmentStatus); ok {
					status.Segment = "test-segment"
					status.LastSequence = 1
				}
				<-ctx.Done()
				return ctx.Err()
			}
			return stream, nil
		},
	}

	c := NewClient(provider)
	ctx := context.Background()
	storeID := uuid.New()

	handler := func(status *SegmentStatus) {
		// Handler just receives the status
	}

	// Act: create subscription
	sub, err := c.SubscribeToSegment(ctx, storeID, "space", "segment", handler)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Act: get subscription status via helper method (Issue 4)
	clientInst := c.(*client)
	ids := clientInst.snapshotSubscriptionIDs()
	require.NotEmpty(t, ids, "should have subscription in registry")
	subID := ids[0]

	// Get status using the helper method
	status := clientInst.GetSubscriptionStatus(subID)

	// Assert: subscription should be tracked and queryable
	assert.NotNil(t, status, "should be able to query subscription status (Issue 4)")
	if status != nil {
		assert.Equal(t, "active", status.Status, "active subscriptions should report an active status")
	}

	// Cleanup
	sub.Unsubscribe()
}

// TestContextCancellationOnUnsubscribe tests Issue 5: Context mismatch
func TestShouldContextCancellationOnUnsubscribe(t *testing.T) {
	// Arrange
	// This tests Issue #5: Context Inheritance Mismatch
	// Unsubscribe should properly cancel subscription context

	decodeCalls := 0
	decodecallsMu := sync.Mutex{}

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}

			// Block indefinitely to simulate long-running stream
			stream.decodeFn = func(m any) error {
				decodecallsMu.Lock()
				decodeCalls++
				decodecallsMu.Unlock()

				<-ctx.Done() // Wait for context cancellation
				return ctx.Err()
			}

			return stream, nil
		},
	}

	c := NewClient(provider)
	ctx := context.Background()
	storeID := uuid.New()

	handler := func(status *SegmentStatus) {
		// Handler just receives the status
	}

	// Act: create subscription
	sub, err := c.SubscribeToSegment(ctx, storeID, "space", "segment", handler)
	require.NoError(t, err)

	// Give subscription time to connect
	time.Sleep(100 * time.Millisecond)

	// Act: Unsubscribe should cancel subscription context (Issue 5)
	sub.Unsubscribe()

	// Give goroutines time to stop
	time.Sleep(100 * time.Millisecond)

	// Assert: After unsubscribe, decode should have been called and blocked,
	// then context canceled when Unsubscribe() was called
	decodecallsMu.Lock()
	calls := decodeCalls
	decodecallsMu.Unlock()

	// Should have attempted at least one decode before context was canceled
	assert.Greater(t, calls, 0, "subscription should have connected and attempted decode")
}

// TestSubscriptionCleanupOnPanic tests Issue 6: Registry cleanup on handler panic
func TestShouldSubscriptionCleanupOnPanic(t *testing.T) {
	// Arrange
	// This tests Issue #6: Registry Never Cleaned on Handler Panic
	// With panic recovery added (Issue 1), subscriptions should not crash entirely
	// but should have proper cleanup semantics

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			// Return context done after first decode
			stream.decodeFn = func(m any) error {
				if status, ok := m.(*SegmentStatus); ok {
					status.Segment = "test-segment"
					status.LastSequence = 1
				}
				return nil
			}
			return stream, nil
		},
	}

	c := NewClient(provider)
	ctx := context.Background()
	storeID := uuid.New()

	handlerCalls := 0
	handlerCallsMu := sync.Mutex{}

	// Handler that panics initially, then works
	handler := func(status *SegmentStatus) {
		handlerCallsMu.Lock()
		handlerCalls++
		currentCall := handlerCalls
		handlerCallsMu.Unlock()

		if currentCall == 1 {
			panic("intentional panic")
		}
	}

	// Act: create subscription
	sub, err := c.SubscribeToSegment(ctx, storeID, "space", "segment", handler)
	require.NoError(t, err)

	// Give subscription time to connect and call handler
	time.Sleep(200 * time.Millisecond)

	// Act: Verify subscription is still registered despite panic (Issue 1: panic recovery working)
	clientInst := c.(*client)
	countAfterPanic := clientInst.subscriptionCount()

	// Assert:  Subscription should still be in registry (panic was recovered)
	assert.Greater(t, countAfterPanic, 0, "subscription should survive panic with recovery (Issue 1 + Issue 6)")

	// Cleanup
	sub.Unsubscribe()

	// After unsubscribe, subscription should be removed from registry
	countAfterUnsubscribe := clientInst.subscriptionCount()

	assert.Equal(t, 0, countAfterUnsubscribe, "subscription should be unregistered after Unsubscribe")
}

func TestShouldOffsetTracking(t *testing.T) {
	// Arrange
	// This tests Critical Issue #3: No Offset Tracking
	// We verify that subscriptions track the last delivered sequence

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}

			callCount := 0
			stream.decodeFn = func(m any) error {
				callCount++
				if status, ok := m.(*SegmentStatus); ok {
					status.Segment = "test-segment"
					status.LastSequence = uint64(callCount * 10) // 10, 20, 30...
				}

				if callCount >= 2 {
					<-ctx.Done()
					return ctx.Err()
				}

				return nil
			}

			return stream, nil
		},
	}

	c := NewClient(provider)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storeID := uuid.New()

	maxSequenceSeen := int64(0)
	maxSequenceSeenMu := sync.Mutex{}

	handler := func(status *SegmentStatus) {
		maxSequenceSeenMu.Lock()
		if status.LastSequence > uint64(maxSequenceSeen) {
			maxSequenceSeen = int64(status.LastSequence)
		}
		maxSequenceSeenMu.Unlock()
	}

	// Act: create subscription
	sub, err := c.SubscribeToSegment(ctx, storeID, "space", "segment", handler)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	// Assert: subscription should track offset
	clientInst := c.(*client)
	var lastDelivered int64
	for _, activeSub := range clientInst.snapshotSubscriptions() {
		delivered := activeSub.lastDelivered.Load()
		if delivered > lastDelivered {
			lastDelivered = delivered
		}
	}

	// Should have tracked at least one sequence
	assert.Greater(t, lastDelivered, int64(0), "should track delivered sequences")

	// Cleanup
	sub.Unsubscribe()
}

// TestHandlerTimeoutTracking verifies that slow handlers don't block subscriptions (Issue 7).
// Handlers that exceed the timeout should be interrupted while maintaining message delivery.
func TestShouldHandlerTimeoutTracking(t *testing.T) {
	// Arrange
	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			callCount := 0
			stream.decodeFn = func(m any) error {
				callCount++
				if callCount <= 3 {
					if status, ok := m.(*SegmentStatus); ok {
						*status = SegmentStatus{LastSequence: uint64(callCount)}
					}
					return nil
				}
				<-ctx.Done()
				return ctx.Err()
			}

			return stream, nil
		},
	}

	// Use very short timeout to test timeout behavior
	c := NewClientWithHandlerTimeout(provider, 50*time.Millisecond)
	ctx := context.Background()
	storeID := uuid.New()

	handlerCalls := int32(0)

	// Handler that sleeps (exceeds timeout)
	handler := func(status *SegmentStatus) {
		atomic.AddInt32(&handlerCalls, 1)
		// Sleep longer than timeout to trigger timeout
		time.Sleep(200 * time.Millisecond)
	}

	// Act: create subscription
	sub, err := c.SubscribeToSegment(ctx, storeID, "space", "segment", handler)
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	// Assert: handler should have been called multiple times despite timeouts
	assert.Greater(t, atomic.LoadInt32(&handlerCalls), int32(0), "should call handler despite timeout")

	// Cleanup
	sub.Unsubscribe()
}

func TestShouldHandlerTimeoutCanBeDisabled(t *testing.T) {
	c := NewClientWithHandlerTimeout(&mockProvider{}, 0).(*client)
	t.Cleanup(func() {
		require.NoError(t, c.Close())
	})

	sub := &activeSubscription{}
	started := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan struct{})

	go func() {
		c.runHandler(context.Background(), sub, "sub-id", func(*SegmentStatus) {
			close(started)
			<-release
		}, &SegmentStatus{Space: "space", Segment: "segment", LastSequence: 1})
		close(returned)
	}()

	<-started
	select {
	case <-returned:
		t.Fatal("runHandler should wait for handler completion when timeout is disabled")
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("runHandler should return after the handler completes")
	}

	assert.Equal(t, int32(0), sub.handlerTimeouts.Load(), "disabled timeout should not increment timeout metrics")
}

// TestHandlerTimeoutMetrics verifies that handler timeouts and panics are tracked (Issue 7).
// Applications should be able to query subscription health including timeout/panic counts.
func TestShouldHandlerTimeoutMetrics(t *testing.T) {
	// Arrange
	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			callCount := 0
			stream.decodeFn = func(m any) error {
				callCount++
				if callCount <= 3 {
					if status, ok := m.(*SegmentStatus); ok {
						*status = SegmentStatus{LastSequence: uint64(callCount)}
					}
					return nil
				}
				<-ctx.Done()
				return ctx.Err()
			}

			return stream, nil
		},
	}

	// Use short timeout
	c := NewClientWithHandlerTimeout(provider, 50*time.Millisecond)
	ctx := context.Background()
	storeID := uuid.New()

	callCount := int32(0)

	// Handler that sometimes sleeps (timeout) and sometimes panics
	handler := func(status *SegmentStatus) {
		call := atomic.AddInt32(&callCount, 1)
		if call%2 == 0 {
			// Sleep to trigger timeout on even calls
			time.Sleep(200 * time.Millisecond)
		} else if call == 3 {
			// Panic on third call to test panic tracking
			panic("test panic")
		}
	}

	// Act: create subscription
	sub, err := c.SubscribeToSegment(ctx, storeID, "space", "segment", handler)
	require.NoError(t, err)

	// Get subscription ID from internal registry
	clientInst := c.(*client)
	ids := clientInst.snapshotSubscriptionIDs()
	require.NotEmpty(t, ids, "should have subscription in registry")
	subID := ids[0]

	require.NotEmpty(t, subID, "should have subscription in registry")

	time.Sleep(300 * time.Millisecond)

	// Assert: GetSubscriptionStatus should report timeout and panic metrics
	status := c.GetSubscriptionStatus(subID)
	require.NotNil(t, status, "should return subscription status")
	assert.Greater(t, status.HandlerTimeouts, int32(0), "should track handler timeouts")
	// Handler panics may also be tracked
	t.Logf("Handler timeouts: %d, panics: %d", status.HandlerTimeouts, status.HandlerPanics)

	// Cleanup
	sub.Unsubscribe()
}

// TestSlowHandlerDoesNotBlockOtherMessages verifies handler backpressure (Issue 7).
// Subscription receiving messages should continue processing even if handler is slow.
func TestShouldSlowHandlerDoesNotBlockOtherMessages(t *testing.T) {
	// Arrange - test with slow handler that times out
	callCount := int32(0)
	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			seqNum := 0
			stream.decodeFn = func(m any) error {
				seqNum++
				// Generate many messages quickly
				if seqNum <= 10 {
					if status, ok := m.(*SegmentStatus); ok {
						*status = SegmentStatus{LastSequence: uint64(seqNum)}
					}
					return nil
				}
				return errors.New("stream ended")
			}

			return stream, nil
		},
	}

	// Use short timeout
	c := NewClientWithHandlerTimeout(provider, 50*time.Millisecond)

	ctx := context.Background()
	storeID := uuid.New()

	// Slow handler (exceeds timeout)
	slowHandler := func(status *SegmentStatus) {
		atomic.AddInt32(&callCount, 1)
		time.Sleep(200 * time.Millisecond) // Much longer than timeout
	}

	// Act: create subscription
	sub, err := c.SubscribeToSegment(ctx, storeID, "space", "segment", slowHandler)
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// Assert: handler should have been called multiple times despite timeouts
	// If handler was blocking, we'd only see ~2 calls in 500ms (500/200=2.5)
	// With timeout, we should see more calls because timeouts don't block
	calls := atomic.LoadInt32(&callCount)
	assert.Greater(t, calls, int32(0), "handler should be called")
	t.Logf("Handler called %d times with 50ms timeout and 200ms sleep", calls)

	// Cleanup
	sub.Unsubscribe()
}

func TestShouldSubscriptionCoalescesLatestStatusWhenHandlersSaturate(t *testing.T) {
	var decodeCount atomic.Int32
	firstHandlerStarted := make(chan struct{})
	releaseFirstHandler := make(chan struct{})
	received := make(chan uint64, 4)

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
				status, ok := m.(*SegmentStatus)
				if !ok {
					return errors.New("unexpected decode target")
				}

				seq := decodeCount.Add(1)
				if seq > 1 {
					<-firstHandlerStarted
				}
				if seq <= 20 {
					*status = SegmentStatus{Space: "space", Segment: "segment", LastSequence: uint64(seq)}
					return nil
				}

				<-ctx.Done()
				return ctx.Err()
			}
			return stream, nil
		},
	}

	c := NewClientWithHandlerTimeout(provider, time.Second).(*client)
	c.maxConcurrentHandlers = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := func(status *SegmentStatus) {
		received <- status.LastSequence
		if status.LastSequence == 1 {
			select {
			case <-firstHandlerStarted:
			default:
				close(firstHandlerStarted)
			}
			<-releaseFirstHandler
		}
	}

	sub, err := c.SubscribeToSegment(ctx, uuid.New(), "space", "segment", handler)
	require.NoError(t, err)
	subID := sub.ID()
	require.NotEmpty(t, subID)
	pendingStatuses := 0

	select {
	case <-firstHandlerStarted:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for the first handler invocation")
	}

	require.Eventually(t, func() bool {
		status := c.GetSubscriptionStatus(subID)
		if status == nil {
			return false
		}
		pendingStatuses = status.PendingStatuses
		return status.CoalescedUpdates > 0 && status.PendingStatuses > 0
	}, time.Second, 10*time.Millisecond)
	assert.Equal(t, 1, pendingStatuses, "single-segment subscriptions should expose one pending latest status while saturated")

	require.Eventually(t, func() bool {
		return decodeCount.Load() >= 20
	}, time.Second, 10*time.Millisecond)

	close(releaseFirstHandler)

	var seen []uint64
	require.Eventually(t, func() bool {
		for len(seen) < 2 {
			select {
			case seq := <-received:
				seen = append(seen, seq)
			default:
				return false
			}
		}
		return true
	}, time.Second, 10*time.Millisecond)

	assert.Equal(t, []uint64{1, 20}, seen[:2], "handler should receive the first and then the latest coalesced status")

	status := c.GetSubscriptionStatus(subID)
	require.NotNil(t, status)
	assert.Greater(t, status.CoalescedUpdates, int64(0), "subscription status should report coalesced updates")
	assert.Equal(t, 0, status.PendingStatuses, "pending status count should drain after handlers catch up")

	cancel()
	sub.Unsubscribe()
}

func TestShouldSubscriptionStatusHandlesDifferentConcreteErrorTypes(t *testing.T) {
	c := NewClient(&mockProvider{}).(*client)
	sub := &activeSubscription{id: "sub-1"}
	sub.status.Store("failed")
	sub.setLastError(errors.New("first failure"))
	sub.setLastError(context.DeadlineExceeded)

	c.registerSubscription(sub.id, sub)

	status := c.GetSubscriptionStatus(sub.id)
	require.NotNil(t, status)
	require.NotNil(t, status.LastError)
	assert.Equal(t, "failed", status.Status)
	assert.Equal(t, context.DeadlineExceeded.Error(), status.LastError.Error())
}

func TestShouldExactSubscriptionUsesSinglePendingStatusSlot(t *testing.T) {
	sub := &activeSubscription{}

	coalesced := sub.queuePendingStatus(SegmentStatus{Space: "space", Segment: "segment", LastSequence: 1})
	assert.False(t, coalesced)
	assert.Nil(t, sub.pendingStatusByKey)
	assert.Empty(t, sub.pendingOrder)
	assert.Equal(t, 1, sub.pendingStatusCount())

	coalesced = sub.queuePendingStatus(SegmentStatus{Space: "space", Segment: "segment", LastSequence: 2})
	assert.True(t, coalesced)
	assert.Nil(t, sub.pendingStatusByKey)
	assert.Empty(t, sub.pendingOrder)

	status, ok := sub.takePendingStatus()
	require.True(t, ok)
	assert.Equal(t, uint64(2), status.LastSequence)
	assert.Equal(t, 0, sub.pendingStatusCount())

	_, ok = sub.takePendingStatus()
	assert.False(t, ok)
}

func TestShouldSpaceSubscriptionRetainsLatestPendingStatusPerSegmentWhenHandlersSaturate(t *testing.T) {
	var decodeCount atomic.Int32
	firstHandlerStarted := make(chan struct{})
	releaseFirstHandler := make(chan struct{})
	received := make(chan string, 8)
	statuses := []SegmentStatus{
		{Space: "space", Segment: "seg-a", LastSequence: 1},
		{Space: "space", Segment: "seg-a", LastSequence: 2},
		{Space: "space", Segment: "seg-b", LastSequence: 1},
		{Space: "space", Segment: "seg-c", LastSequence: 1},
		{Space: "space", Segment: "seg-b", LastSequence: 2},
	}

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
				status, ok := m.(*SegmentStatus)
				if !ok {
					return errors.New("unexpected decode target")
				}

				index := int(decodeCount.Add(1)) - 1
				if index > 0 {
					<-firstHandlerStarted
				}
				if index < len(statuses) {
					*status = statuses[index]
					return nil
				}

				<-ctx.Done()
				return ctx.Err()
			}
			return stream, nil
		},
	}

	c := NewClientWithHandlerTimeout(provider, time.Second).(*client)
	c.maxConcurrentHandlers = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := func(status *SegmentStatus) {
		received <- fmt.Sprintf("%s:%d", status.Segment, status.LastSequence)
		if status.Segment == "seg-a" && status.LastSequence == 1 {
			select {
			case <-firstHandlerStarted:
			default:
				close(firstHandlerStarted)
			}
			<-releaseFirstHandler
		}
	}

	sub, err := c.SubscribeToSpace(ctx, uuid.New(), "space", handler)
	require.NoError(t, err)
	subID := sub.ID()
	require.NotEmpty(t, subID)
	pendingStatuses := 0

	select {
	case <-firstHandlerStarted:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for the first space-subscription handler invocation")
	}

	require.Eventually(t, func() bool {
		return decodeCount.Load() >= int32(len(statuses))
	}, time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		status := c.GetSubscriptionStatus(subID)
		if status == nil {
			return false
		}
		pendingStatuses = status.PendingStatuses
		return status.CoalescedUpdates > 0 && status.PendingStatuses >= 3
	}, time.Second, 10*time.Millisecond)
	assert.Equal(t, 3, pendingStatuses, "space subscriptions should expose one pending latest status per segment while saturated")

	close(releaseFirstHandler)

	var seen []string
	require.Eventually(t, func() bool {
		for len(seen) < 4 {
			select {
			case update := <-received:
				seen = append(seen, update)
			default:
				return false
			}
		}
		return true
	}, time.Second, 10*time.Millisecond)

	assert.Equal(t, []string{"seg-a:1", "seg-a:2", "seg-b:2", "seg-c:1"}, seen[:4], "space subscription should retain the latest pending status for each segment")

	status := c.GetSubscriptionStatus(subID)
	require.NotNil(t, status)
	assert.Equal(t, 0, status.PendingStatuses, "pending status count should drain after handlers catch up")

	cancel()
	sub.Unsubscribe()
}

// TestConsumeSegmentResilient verifies that ConsumeSegment wraps with resilience (Issue 8)
func TestShouldConsumeSegmentResilient(t *testing.T) {
	// Just verify that ConsumeSegment returns an enumerator
	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
				return errors.New("stream ended")
			}
			return stream, nil
		},
	}

	c := NewClient(provider)
	ctx := context.Background()
	storeID := uuid.New()

	// Act: get enumerator
	args := &ConsumeSegment{Space: "space", Segment: "segment"}
	enum := c.ConsumeSegment(ctx, storeID, args)
	defer enum.Dispose()

	// Assert: enumerator exists and can be used (MoveNext returns false since stream has no data)
	assert.NotNil(t, enum)
	// The first MoveNext will return false because stream ends immediately
	assert.False(t, enum.MoveNext())
}

// TestConsumeSpaceResilient verifies that ConsumeSpace wraps with resilience (Issue 8)
func TestShouldConsumeSpaceResilient(t *testing.T) {
	// Just verify that ConsumeSpace returns an enumerator
	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
				return errors.New("stream ended")
			}
			return stream, nil
		},
	}

	c := NewClient(provider)
	ctx := context.Background()
	storeID := uuid.New()

	// Act: get enumerator
	args := &ConsumeSpace{Space: "space"}
	enum := c.ConsumeSpace(ctx, storeID, args)
	defer enum.Dispose()

	// Assert: enumerator exists
	assert.NotNil(t, enum)
	assert.False(t, enum.MoveNext())
}

// TestConsumeResilient verifies that Consume wraps with resilience (Issue 8)
func TestShouldConsumeResilient(t *testing.T) {
	// Just verify that Consume returns an enumerator
	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
				return errors.New("stream ended")
			}
			return stream, nil
		},
	}

	c := NewClient(provider)
	ctx := context.Background()
	storeID := uuid.New()

	// Act: get enumerator
	args := &Consume{}
	enum := c.Consume(ctx, storeID, args)
	defer enum.Dispose()

	// Assert: enumerator exists
	assert.NotNil(t, enum)
	assert.False(t, enum.MoveNext())
}

// TestProduceSafetyAtomic verifies that Peek+Produce is atomic (Issue 9).
// Concurrent publishers to the same segment should not create sequence conflicts.
func TestShouldProduceSafetyAtomic(t *testing.T) {
	var peekSequence uint64 = 100
	var callCount int32
	sequences := make(chan uint64, 16)

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}

			// Handle Peek
			if _, ok := routeable.(*api.Peek); ok {
				stream.decodeFn = func(m interface{}) error {
					if entry, ok := m.(*Entry); ok {
						*entry = Entry{Sequence: atomic.LoadUint64(&peekSequence)}
					}
					return nil
				}
				return stream, nil
			}

			// Handle Produce
			if _, ok := routeable.(*api.Produce); ok {
				atomic.AddInt32(&callCount, 1)
				stream.encodeFn = func(m interface{}) error {
					if rec, ok := m.(*Record); ok {
						// record produced sequence and advance server-side sequence
						sequences <- rec.Sequence
						atomic.StoreUint64(&peekSequence, rec.Sequence)
					}
					return nil
				}
				var decoded int32
				stream.decodeFn = func(m interface{}) error {
					call := atomic.AddInt32(&decoded, 1)
					if call == 1 {
						if status, ok := m.(*SegmentStatus); ok {
							*status = SegmentStatus{Space: "test", Segment: "seg"}
						}
						return nil
					}
					return errors.New("stream ended")
				}
			}

			return stream, nil
		},
	}

	c := NewClient(provider)
	ctx := context.Background()
	storeID := uuid.New()

	// Act: Issue concurrent Publish calls
	var wg sync.WaitGroup
	var errsMu sync.Mutex
	var errs []error
	numPublishers := 5
	for i := 0; i < numPublishers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Publish(ctx, storeID, "space", "seg", []byte("test"), nil); err != nil {
				errsMu.Lock()
				errs = append(errs, err)
				errsMu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(sequences)

	// Assert: All Publish calls succeeded
	require.Empty(t, errs)

	// Collect produced sequences
	got := make([]uint64, 0, numPublishers)
	for s := range sequences {
		got = append(got, s)
	}

	require.Len(t, got, numPublishers)
	// Verify sequences are strictly increasing
	for i := 1; i < len(got); i++ {
		assert.Greater(t, got[i], got[i-1], "produced sequences should be strictly increasing")
	}
	assert.Equal(t, int32(numPublishers), atomic.LoadInt32(&callCount), "all publishes should call Produce")
}

// TestProduceLockPreventsRaceCondition verifies that concurrent Peek+Produce operations
// on the same segment don't race (Issue 9).
func TestShouldProduceLockPreventsRaceCondition(t *testing.T) {
	var lastProducedSequence uint64

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}

			if peek, ok := routeable.(*api.Peek); ok {
				_ = peek
				stream.decodeFn = func(m interface{}) error {
					if entry, ok := m.(*Entry); ok {
						// Each peek returns current sequence
						seq := atomic.LoadUint64(&lastProducedSequence)
						*entry = Entry{Sequence: seq}
					}
					return nil
				}
				return stream, nil
			}

			if prod, ok := routeable.(*api.Produce); ok {
				_ = prod
				stream.encodeFn = func(m interface{}) error {
					if rec, ok := m.(*Record); ok {
						// Verify sequences are monotonically increasing
						assert.Greater(t, rec.Sequence, atomic.LoadUint64(&lastProducedSequence),
							"published sequence should be greater than last")
						atomic.StoreUint64(&lastProducedSequence, rec.Sequence)
					}
					return nil
				}
				stream.decodeFn = func(m interface{}) error {
					if status, ok := m.(*SegmentStatus); ok {
						*status = SegmentStatus{Space: "sp", Segment: "s"}
					}
					return nil
				}
			}

			return stream, nil
		},
	}

	c := NewClient(provider)
	ctx := context.Background()
	storeID := uuid.New()

	// Act: Concurrent publishes with dynamic peek
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.Publish(ctx, storeID, "sp", "s", []byte("data"), nil)
		}()
	}
	wg.Wait()

	// Assert: Final sequence should be correct
	finalSeq := atomic.LoadUint64(&lastProducedSequence)
	assert.Equal(t, uint64(10), finalSeq, "10 sequential publishes should result in sequence 10")
}

// TestProduceLockIsolatesBySegment verifies that locks are per-segment, not global (Issue 9).
// Publishers to different segments should not block each other.
func TestShouldProduceLockIsolatesBySegment(t *testing.T) {
	var seg1Count, seg2Count int32
	var mu sync.Mutex
	started := sync.NewCond(&mu)
	allStarted := false

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}

			if peek, ok := routeable.(*api.Peek); ok {
				_ = peek
				stream.decodeFn = func(m interface{}) error {
					if entry, ok := m.(*Entry); ok {
						*entry = Entry{Sequence: 100}
					}
					return nil
				}
				return stream, nil
			}

			if prod, ok := routeable.(*api.Produce); ok {
				// Wait for all goroutines to start before proceeding
				mu.Lock()
				for !allStarted {
					started.Wait()
				}
				mu.Unlock()

				switch prod.Segment {
				case "seg1":
					atomic.AddInt32(&seg1Count, 1)
				case "seg2":
					atomic.AddInt32(&seg2Count, 1)
				}

				stream.encodeFn = func(m interface{}) error { return nil }
				stream.decodeFn = func(m interface{}) error {
					if status, ok := m.(*SegmentStatus); ok {
						*status = SegmentStatus{}
					}
					return nil
				}
			}

			return stream, nil
		},
	}

	c := NewClient(provider)
	ctx := context.Background()
	storeID := uuid.New()

	// Act: Publish to segment 1 and segment 2 concurrently
	// (no blocking by different segment locks)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = c.Publish(ctx, storeID, "sp", "seg1", []byte("data"), nil)
	}()
	go func() {
		defer wg.Done()
		_ = c.Publish(ctx, storeID, "sp", "seg2", []byte("data"), nil)
	}()

	// Signal that both goroutines have started
	mu.Lock()
	allStarted = true
	started.Broadcast()
	mu.Unlock()

	wg.Wait()

	// Assert: Both segments had their Produce called
	assert.Greater(t, atomic.LoadInt32(&seg1Count), int32(0), "segment 1 should have been produced")
	assert.Greater(t, atomic.LoadInt32(&seg2Count), int32(0), "segment 2 should have been produced")
}

// --- New tests for RC hardening ---

func TestShouldConsumeSegmentStopsAfterDisconnectWithoutRetry(t *testing.T) {
	var calls atomic.Int32

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			calls.Add(1)
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			seq := 0
			stream.decodeFn = func(m any) error {
				if v, ok := m.(*api.Entry); ok {
					seq++
					if seq == 1 {
						*v = api.Entry{
							Space:     "space",
							Segment:   "segment",
							Sequence:  1,
							Timestamp: 1,
							TRX:       api.TRX{ID: uuid.New(), Number: 1},
							Payload:   []byte("payload"),
						}
						return nil
					}
				}
				return errors.New("simulated disconnect")
			}
			return stream, nil
		},
	}

	c := NewClient(provider)
	args := &ConsumeSegment{Space: "space", Segment: "segment", MinSequence: 7}
	enum := c.ConsumeSegment(context.Background(), uuid.New(), args)
	defer enum.Dispose()

	assert.True(t, enum.MoveNext())
	entry, err := enum.Current()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), entry.Sequence)

	assert.False(t, enum.MoveNext())
	require.Error(t, enum.Err())
	assert.ErrorContains(t, enum.Err(), "simulated disconnect")
	assert.Equal(t, int32(1), calls.Load(), "consume streams should not reopen after a disconnect")
	assert.Equal(t, uint64(7), args.MinSequence, "caller offsets should remain unchanged after a disconnect")
}

func TestShouldConsumeReturnsNoEntryWhenStreamEndsImmediately(t *testing.T) {
	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(any) error {
				return io.EOF
			}
			return stream, nil
		},
	}

	c := NewClient(provider)
	enum := c.Consume(context.Background(), uuid.New(), &Consume{
		Offsets: map[string]lexkey.LexKey{"tenants": nil},
	})
	defer enum.Dispose()

	assert.False(t, enum.MoveNext())
	require.NoError(t, enum.Err())
}

func TestShouldConsumeRejectsZeroValueEntry(t *testing.T) {
	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
				if entry, ok := m.(*api.Entry); ok {
					*entry = api.Entry{}
					return nil
				}
				return errors.New("unexpected decode target")
			}
			return stream, nil
		},
	}

	c := NewClient(provider)
	enum := c.Consume(context.Background(), uuid.New(), &Consume{
		Offsets: map[string]lexkey.LexKey{"tenants": nil},
	})
	defer enum.Dispose()

	assert.False(t, enum.MoveNext())
	require.ErrorIs(t, enum.Err(), errInvalidConsumeEntry)
}

func TestShouldConsumeDoesNotMutateCallerOffsetsOnDisconnect(t *testing.T) {
	var calls atomic.Int32
	initialOffset := lexkey.Encode("space", "segment", 1)

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			calls.Add(1)
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(any) error {
				return errors.New("simulated disconnect")
			}
			return stream, nil
		},
	}

	args := &Consume{Offsets: map[string]lexkey.LexKey{"space": initialOffset}}
	c := NewClient(provider)
	enum := c.Consume(context.Background(), uuid.New(), args)
	defer enum.Dispose()

	assert.False(t, enum.MoveNext())
	require.Error(t, enum.Err())
	assert.ErrorContains(t, enum.Err(), "simulated disconnect")
	assert.Equal(t, int32(1), calls.Load(), "consume enumerators should not retry after a disconnect")
	assert.Equal(t, initialOffset, args.Offsets["space"], "consume arguments should remain owned by the caller")
}

func TestShouldProduceLockEviction(t *testing.T) {
	prev := MaxProduceLocks
	MaxProduceLocks = 100 // shrink for test
	defer func() { MaxProduceLocks = prev }()

	provider := &mockProvider{}
	c := NewClient(provider).(*client)

	storeID := uuid.New()
	// Create more than MaxProduceLocks unique locks
	for i := 0; i < 150; i++ {
		space := fmt.Sprintf("s%d", i)
		segment := fmt.Sprintf("seg%d", i)
		_ = c.getProduceLock(storeID, space, segment)
	}

	// Ensure the map size does not exceed MaxProduceLocks
	c.produceLocksLock.RLock()
	size := len(c.produceLocks)
	c.produceLocksLock.RUnlock()
	assert.LessOrEqual(t, size, MaxProduceLocks)
}

func TestShouldPeekReturnsPermanentRemoteErrorWithoutRetry(t *testing.T) {
	var calls atomic.Int32
	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			calls.Add(1)
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(any) error {
				return errors.New("remote error: access denied")
			}
			return stream, nil
		},
	}

	c := NewClient(provider)
	_, err := c.Peek(context.Background(), uuid.New(), "space", "segment")

	require.Error(t, err)
	assert.ErrorContains(t, err, "access denied")
	assert.Equal(t, int32(1), calls.Load(), "permanent remote errors should not trigger unary retries")
}

func TestShouldConsumeSegmentReturnsPermanentRemoteErrorWithoutRetry(t *testing.T) {
	var calls atomic.Int32
	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			calls.Add(1)
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(any) error {
				return errors.New("remote error: access denied")
			}
			return stream, nil
		},
	}

	c := NewClient(provider)
	enum := c.ConsumeSegment(context.Background(), uuid.New(), &ConsumeSegment{
		Space:   "space",
		Segment: "segment",
	})
	defer enum.Dispose()

	assert.False(t, enum.MoveNext())
	require.Error(t, enum.Err())
	assert.ErrorContains(t, enum.Err(), "access denied")
	assert.Equal(t, int32(1), calls.Load(), "permanent stream errors should not trigger consume retries")
}

func TestShouldSubscriptionInitialCallStreamReturnsErrorAfterRetries(t *testing.T) {
	var attempts atomic.Int32

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			attempts.Add(1)
			return nil, errors.New("can't connect")
		},
	}

	c := NewClientWithRetryPolicy(provider, RetryPolicy{
		MaxAttempts:       3,
		InitialBackoff:    time.Millisecond,
		MaxBackoff:        time.Millisecond,
		BackoffMultiplier: 1,
	}).(*client)

	sub, err := c.SubscribeToSegment(context.Background(), uuid.New(), "space", "segment", func(*SegmentStatus) {})
	require.Error(t, err)
	assert.Nil(t, sub)
	assert.Equal(t, int32(3), attempts.Load(), "initial subscription open should use retry policy, then return the final error")

	assert.Equal(t, 0, c.subscriptionCount(), "failed initial subscription opens must not leave registry entries behind")
}

func TestShouldSubscriptionStopsOnPermanentRemoteError(t *testing.T) {
	var attempts atomic.Int32
	handlerCalled := make(chan struct{}, 1)

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			attempts.Add(1)
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(any) error {
				return errors.New("remote error: access denied")
			}
			return stream, nil
		},
	}

	c := NewClientWithHandlerTimeout(provider, 50*time.Millisecond).(*client)
	sub, err := c.SubscribeToSegment(context.Background(), uuid.New(), "space", "segment", func(*SegmentStatus) {
		select {
		case handlerCalled <- struct{}{}:
		default:
		}
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return c.subscriptionCount() == 0
	}, time.Second, 10*time.Millisecond, "permanent remote errors should stop the subscription loop")

	assert.Equal(t, int32(1), attempts.Load(), "permanent remote errors should not trigger reconnect retries")
	select {
	case <-handlerCalled:
		t.Fatal("handler should not run for permanent remote errors")
	default:
	}

	sub.Unsubscribe()
}

func TestShouldSubscriptionCancelsOnPermanentCallStreamError(t *testing.T) {
	var attempts atomic.Int32
	handlerCalled := make(chan struct{}, 1)

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			attempts.Add(1)
			return nil, errors.New("access denied")
		},
	}

	c := NewClientWithHandlerTimeout(provider, 50*time.Millisecond).(*client)
	sub, err := c.SubscribeToSegment(context.Background(), uuid.New(), "space", "segment", func(*SegmentStatus) {
		select {
		case handlerCalled <- struct{}{}:
		default:
		}
	})
	require.Error(t, err)
	assert.Nil(t, sub)

	assert.Equal(t, int32(1), attempts.Load(), "permanent CallStream errors should not trigger reconnect retries")
	select {
	case <-handlerCalled:
		t.Fatal("handler should not run when CallStream fails permanently")
	default:
	}

	assert.Equal(t, 0, c.subscriptionCount(), "failed initial subscription opens must not register a subscription")
}

func TestShouldSubscriptionStopsOnStreamOverloadedError(t *testing.T) {
	var attempts atomic.Int32
	handlerCalled := make(chan struct{}, 1)

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			attempts.Add(1)
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(any) error {
				return fmt.Errorf("remote error: %s", wskit.ErrStreamOverloaded)
			}
			return stream, nil
		},
	}

	c := NewClientWithHandlerTimeout(provider, 50*time.Millisecond).(*client)
	sub, err := c.SubscribeToSegment(context.Background(), uuid.New(), "space", "segment", func(*SegmentStatus) {
		select {
		case handlerCalled <- struct{}{}:
		default:
		}
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return c.subscriptionCount() == 0
	}, time.Second, 10*time.Millisecond, "stream overload should stop the subscription loop")

	assert.Equal(t, int32(1), attempts.Load(), "stream overload should not trigger reconnect retries")
	select {
	case <-handlerCalled:
		t.Fatal("handler should not run after stream overload")
	default:
	}

	sub.Unsubscribe()
}

func TestShouldRetryFailedSubscriptionReturnsError(t *testing.T) {
	var attempts atomic.Int32

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			attempts.Add(1)
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
				<-ctx.Done()
				return ctx.Err()
			}
			return stream, nil
		},
	}

	c := NewClient(provider).(*client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := c.SubscribeToSegment(ctx, uuid.New(), "space", "segment", func(*SegmentStatus) {})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return attempts.Load() >= 1
	}, time.Second, 10*time.Millisecond)

	ids := c.snapshotSubscriptionIDs()
	require.NotEmpty(t, ids)
	subID := ids[0]
	require.NotEmpty(t, subID)

	err = c.RetryFailedSubscription(subID)
	require.Error(t, err)
	assert.ErrorContains(t, err, "does not support in-place retry")
	assert.Equal(t, int32(1), attempts.Load(), "retry requests must not reopen active streams")

	cancel()
	sub.Unsubscribe()
}

func TestShouldClientCloseCancelsActiveSubscriptions(t *testing.T) {
	var closedCount atomic.Int32
	connected := make(chan struct{}, 1)

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			select {
			case connected <- struct{}{}:
			default:
			}
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.closeFn = func(err error) {
				closedCount.Add(1)
			}
			stream.decodeFn = func(m any) error {
				<-ctx.Done()
				return ctx.Err()
			}
			return stream, nil
		},
	}

	c := NewClient(provider).(*client)
	sub, err := c.SubscribeToSegment(context.Background(), uuid.New(), "space", "segment", func(status *SegmentStatus) {})
	require.NoError(t, err)

	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for active subscription stream")
	}

	require.Eventually(t, func() bool {
		return c.subscriptionCount() == 1
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, c.Close())

	remainingSubscriptions := c.subscriptionCount()
	assert.Equal(t, 0, remainingSubscriptions, "Close should wait for subscriptions to unregister before returning")
	assert.GreaterOrEqual(t, closedCount.Load(), int32(1), "closing the client should close active streams")

	// Safe after client shutdown; subscription should already be done.
	sub.Unsubscribe()
}

func TestShouldClientCloseReturnsWhenCalledFromHandler(t *testing.T) {
	closeResult := make(chan error, 1)
	connected := make(chan struct{}, 1)
	metrics := &testClientMetrics{}

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			select {
			case connected <- struct{}{}:
			default:
			}
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			emitted := false
			stream.decodeFn = func(m any) error {
				if !emitted {
					emitted = true
					status, ok := m.(*SegmentStatus)
					if !ok {
						return errors.New("unexpected decode target")
					}
					*status = SegmentStatus{Space: "space", Segment: "segment", LastSequence: 1}
					return nil
				}
				<-ctx.Done()
				return ctx.Err()
			}
			return stream, nil
		},
	}

	c := NewClientWithMetrics(provider, 30*time.Second, metrics).(*client)
	sub, err := c.SubscribeToSegment(context.Background(), uuid.New(), "space", "segment", func(status *SegmentStatus) {
		select {
		case closeResult <- c.Close():
		default:
		}
	})
	require.NoError(t, err)

	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for active subscription stream")
	}

	select {
	case err := <-closeResult:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for Close called from a handler")
	}

	remainingSubscriptions := c.subscriptionCount()
	assert.Equal(t, 0, remainingSubscriptions, "Close called from a handler should still drain subscriptions")
	assert.Equal(t, int32(0), metrics.handlerTimeouts.Load(), "shutdown-triggered cancellation should not be recorded as a handler timeout")

	// Safe after client shutdown; subscription should already be done.
	sub.Unsubscribe()
}

// TestWithLeaseNotAcquired verifies that when server returns Ok false on acquire, WithLease returns ErrLeaseNotAcquired.
func TestShouldWithLeaseNotAcquired(t *testing.T) {
	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
				if res, ok := m.(*api.LeaseResult); ok {
					res.Ok = false
					res.Message = "held by other"
				}
				return nil
			}
			return stream, nil
		},
	}
	c := NewClient(provider)
	ctx := context.Background()
	storeID := uuid.New()

	err := c.WithLease(ctx, storeID, "key1", 30*time.Second, func(context.Context) error { return nil })
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrLeaseNotAcquired)
}

// TestWithLeaseRenewAndRelease verifies that WithLease runs the renew loop and sends LeaseRelease when the callback returns.
func TestShouldWithLeaseRenewAndRelease(t *testing.T) {
	var acquireCount, renewCount, releaseCount atomic.Int32
	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			switch routeable.(type) {
			case *api.LeaseAcquire:
				acquireCount.Add(1)
			case *api.LeaseRenew:
				renewCount.Add(1)
			case *api.LeaseRelease:
				releaseCount.Add(1)
			}
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
				if res, ok := m.(*api.LeaseResult); ok {
					res.Ok = true
				}
				return nil
			}
			return stream, nil
		},
	}
	c := NewClient(provider)
	ctx := context.Background()
	storeID := uuid.New()

	// Min TTL is 1s, renew every ttl/2 = 500ms. Hold for ~1.2s to get at least one renew, then return (release).
	err := c.WithLease(ctx, storeID, "key1", 2*time.Second, func(leaseCtx context.Context) error {
		time.Sleep(1200 * time.Millisecond)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), acquireCount.Load(), "should have exactly one LeaseAcquire")
	assert.GreaterOrEqual(t, renewCount.Load(), int32(1), "should have at least one LeaseRenew")
	assert.Equal(t, int32(1), releaseCount.Load(), "should have exactly one LeaseRelease")
}

// TestWithLeaseRunsCallbackThenReleases verifies that WithLease acquires, runs fn with a context, then releases.
func TestShouldWithLeaseRunsCallbackThenReleases(t *testing.T) {
	var acquireCount, releaseCount atomic.Int32
	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			switch routeable.(type) {
			case *api.LeaseAcquire:
				acquireCount.Add(1)
			case *api.LeaseRelease:
				releaseCount.Add(1)
			}
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
				if res, ok := m.(*api.LeaseResult); ok {
					res.Ok = true
				}
				return nil
			}
			return stream, nil
		},
	}
	c := NewClient(provider)
	ctx := context.Background()
	storeID := uuid.New()

	ran := false
	err := c.WithLease(ctx, storeID, "key1", 30*time.Second, func(leaseCtx context.Context) error {
		ran = true
		select {
		case <-leaseCtx.Done():
			t.Fatal("lease context should not be done yet")
		default:
		}
		return nil
	})
	require.NoError(t, err)
	assert.True(t, ran)
	assert.Equal(t, int32(1), acquireCount.Load())
	assert.Equal(t, int32(1), releaseCount.Load())
}

// TestWithLeaseContextCanceledWhenParentCanceled verifies that when the parent context is canceled, fn's context is canceled.
func TestShouldWithLeaseContextCanceledWhenParentCanceled(t *testing.T) {
	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
				if res, ok := m.(*api.LeaseResult); ok {
					res.Ok = true
				}
				return nil
			}
			return stream, nil
		},
	}
	c := NewClient(provider)
	storeID := uuid.New()
	parentCtx, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()

	callbackStarted := make(chan struct{})
	done := make(chan struct{})
	go func() {
		_ = c.WithLease(parentCtx, storeID, "key1", 30*time.Second, func(leaseCtx context.Context) error {
			close(callbackStarted)
			<-leaseCtx.Done()
			close(done)
			return nil
		})
	}()
	<-callbackStarted
	cancelParent()
	select {
	case <-done:
		// fn saw context canceled and returned
	case <-time.After(2 * time.Second):
		t.Fatal("callback context should have been canceled when parent was canceled")
	}
}

func TestShouldIsRetryable(t *testing.T) {
	assert.False(t, IsRetryable(context.Canceled))
	assert.False(t, IsRetryable(fmt.Errorf("not found")))
	assert.False(t, IsRetryable(fmt.Errorf("invalid argument: foo")))
	assert.True(t, IsRetryable(fmt.Errorf("connection refused")))
	assert.True(t, IsRetryable(fmt.Errorf("503 service unavailable")))
}
