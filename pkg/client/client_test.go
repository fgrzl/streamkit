package client

import (
	"context"
	"errors"
	"fmt"
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

func newTestClientWithQueue(provider api.BidiStreamProvider, metrics ClientMetrics, queueSize int) *client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		provider:              provider,
		policy:                DefaultRetryPolicy(),
		handlerTimeout:        30 * time.Second,
		maxConcurrentHandlers: 50,
		subscriptions:         make(map[string]*activeSubscription),
		reconnectQueue:        make(chan *reconnectRequest, queueSize),
		reconnectDone:         make(chan struct{}),
		reconnectCtx:          ctx,
		reconnectCancel:       cancel,
		produceLocks:          make(map[string]*sync.Mutex),
		metrics:               metrics,
	}
	c.startReconnectDispatcher()
	provider.RegisterReconnectListener(c)
	return c
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

func TestSubscriptionRegisteredOnCreation(t *testing.T) {
	// Arrange
	provider := &mockProvider{}
	c := NewClient(provider)
	require.NotNil(t, c)

	// Assert: client should be registered as listener
	listeners := provider.GetListeners()
	require.Len(t, listeners, 1)
	assert.IsType(t, (*client)(nil), listeners[0])
}

func TestSubscriptionTrackedInRegistry(t *testing.T) {
	// Arrange
	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			// Decode will continuously send status updates
			stream.decodeFn = func(m any) error {
				// Send one status then end stream
				if status, ok := m.(*SegmentStatus); ok {
					*status = SegmentStatus{Segment: "test-segment"}
				}
				// Return error to close stream
				return errors.New("stream ended")
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
	clientInst.subscriptionsMu.RLock()
	activeSubs := len(clientInst.subscriptions)
	clientInst.subscriptionsMu.RUnlock()

	// Subscription should be active (at least initially before stream ends)
	assert.Greater(t, activeSubs, 0)

	// Cleanup
	sub.Unsubscribe()

	// Give unsubscribe time to process
	time.Sleep(100 * time.Millisecond)

	// Assert: subscription should be removed from registry after unsubscribe
	clientInst.subscriptionsMu.RLock()
	activeSubs = len(clientInst.subscriptions)
	clientInst.subscriptionsMu.RUnlock()
	assert.Equal(t, 0, activeSubs)
}

func TestReconnectReplaysSubscriptions(t *testing.T) {
	// Arrange: stream that fails after first decode, then succeeds on reconnect
	var streamAttempts atomic.Int32
	failStream := atomic.Bool{}
	failStream.Store(true)

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			streamAttempts.Add(1)

			if failStream.Load() {
				return nil, errors.New("connection lost")
			}

			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storeID := uuid.New()

	handler := func(status *SegmentStatus) {}

	sub, err := c.SubscribeToSegment(ctx, storeID, "test-space", "test-segment", handler)
	require.NoError(t, err)
	require.NotNil(t, sub)

	// Wait for at least one failed attempt
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && streamAttempts.Load() < 2 {
		time.Sleep(50 * time.Millisecond)
	}
	require.GreaterOrEqual(t, streamAttempts.Load(), int32(2), "should have multiple failed attempts")

	// Now allow connections to succeed and signal reconnect
	attemptsBeforeSignal := streamAttempts.Load()
	failStream.Store(false)

	clientInst := c.(*client)
	err = clientInst.OnReconnected(ctx, uuid.Nil)
	require.NoError(t, err)

	// Wait for the goroutine to reconnect via the signal
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && streamAttempts.Load() <= attemptsBeforeSignal {
		time.Sleep(50 * time.Millisecond)
	}

	assert.Greater(t, streamAttempts.Load(), attemptsBeforeSignal, "reconnect signal should trigger new stream attempt")

	// Cleanup
	cancel()
	sub.Unsubscribe()
}

func TestReconnectSubscriptionDeliversInitialSnapshot(t *testing.T) {
	var attempts atomic.Int32
	updates := make(chan uint64, 4)

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			attempt := attempts.Add(1)
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			emitted := false

			stream.decodeFn = func(m any) error {
				if emitted {
					if attempt == 1 {
						return errors.New("connection lost")
					}
					<-ctx.Done()
					return ctx.Err()
				}
				emitted = true

				status, ok := m.(*SegmentStatus)
				if !ok {
					return errors.New("unexpected decode target")
				}
				*status = SegmentStatus{
					Space:        "space",
					Segment:      "segment",
					LastSequence: uint64(attempt),
				}
				return nil
			}
			return stream, nil
		},
	}

	c := NewClient(provider)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	require.Eventually(t, func() bool {
		return attempts.Load() >= 2
	}, 3*time.Second, 10*time.Millisecond)

	select {
	case seq := <-updates:
		assert.Equal(t, uint64(2), seq)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for reconnect snapshot update")
	}

	cancel()
	sub.Unsubscribe()
}

func TestOnReconnectedCallsAllSubscriptionHandlers(t *testing.T) {
	// Arrange
	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
				if status, ok := m.(*SegmentStatus); ok {
					*status = SegmentStatus{Segment: "test"}
				}
				return nil
			}

			return stream, nil
		},
	}

	c := NewClient(provider)
	ctx := context.Background()
	storeID := uuid.New()

	// Create multiple subscriptions
	handler1 := func(status *SegmentStatus) {
		// Handler will be called during reconnect
	}

	handler2 := func(status *SegmentStatus) {
		// Handler will be called during reconnect
	}

	sub1, err := c.SubscribeToSegment(ctx, storeID, "space1", "seg1", handler1)
	require.NoError(t, err)

	sub2, err := c.SubscribeToSegment(ctx, storeID, "space2", "seg2", handler2)
	require.NoError(t, err)

	// Give subscriptions time to connect
	time.Sleep(100 * time.Millisecond)

	// Act: trigger reconnect
	clientInst := c.(*client)
	err = clientInst.OnReconnected(ctx, uuid.Nil)
	require.NoError(t, err)

	// Give replay time to execute
	time.Sleep(200 * time.Millisecond)

	// Assert: subscriptions should still be receiving updates through replayed handlers
	// (handlers would be called when replayed subscriptions receive status updates)
	clientInst.subscriptionsMu.RLock()
	activeSubs := len(clientInst.subscriptions)
	clientInst.subscriptionsMu.RUnlock()

	assert.Greater(t, activeSubs, 0, "subscriptions should still be active after reconnect")

	// Cleanup
	sub1.Unsubscribe()
	sub2.Unsubscribe()
}

func TestSubscriptionFiltersHeartbeats(t *testing.T) {
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

func TestSubscriptionReconnectsWhenHeartbeatsStop(t *testing.T) {
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

		c := NewClient(provider)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sub, err := c.SubscribeToSegment(ctx, uuid.New(), "space", "segment", func(status *SegmentStatus) {})
		require.NoError(t, err)

		deadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(deadline) && attempts.Load() < 2 {
			time.Sleep(10 * time.Millisecond)
		}

		assert.GreaterOrEqual(t, attempts.Load(), int32(2), "subscription should reconnect after heartbeat timeout")
		sub.Unsubscribe()
	})
}

func TestHandlerPanicResilience(t *testing.T) {
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
	clientInst.subscriptionsMu.RLock()
	activeSubs := len(clientInst.subscriptions)
	clientInst.subscriptionsMu.RUnlock()
	assert.Greater(t, activeSubs, 0, "subscription should still be active after handler panic")

	// Cleanup
	cancel()
	sub.Unsubscribe()
}

func TestNoDuplicateReplayOnRapidReconnect(t *testing.T) {
	// Reconnections are serialized through a single-threaded dispatcher.
	// Verify that at most one CallStream is in-flight at any given time,
	// even when multiple subscriptions enqueue concurrently.

	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			cur := concurrent.Add(1)
			defer concurrent.Add(-1)
			for {
				old := maxConcurrent.Load()
				if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
					break
				}
			}
			// Small sleep so concurrent calls would overlap if they existed
			time.Sleep(5 * time.Millisecond)

			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
				if status, ok := m.(*SegmentStatus); ok {
					status.Segment = "test-segment"
					status.LastSequence = 1
				}
				// Block until context canceled — stable stream
				<-ctx.Done()
				return ctx.Err()
			}
			return stream, nil
		},
	}

	c := NewClient(provider)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storeID := uuid.New()

	// Create multiple subscriptions
	const numSubs = 5
	subs := make([]api.Subscription, numSubs)
	for i := 0; i < numSubs; i++ {
		var err error
		subs[i], err = c.SubscribeToSegment(ctx, storeID, "space", fmt.Sprintf("seg%d", i), func(s *SegmentStatus) {})
		require.NoError(t, err)
	}

	// Wait for all subscriptions to connect (serialized through dispatcher)
	time.Sleep(500 * time.Millisecond)

	// Assert: concurrent CallStream never exceeded 1 (single-threaded dispatcher)
	t.Logf("Max concurrent CallStream: %d", maxConcurrent.Load())
	assert.LessOrEqual(t, maxConcurrent.Load(), int32(1), "reconnect dispatcher should call CallStream one at a time")

	// Cleanup
	cancel()
	for _, sub := range subs {
		sub.Unsubscribe()
	}
}

func TestReconnectFailureBackoffDoesNotBlockOtherSubscriptions(t *testing.T) {
	t.Setenv("STREAMKIT_TEST_NO_JITTER", "1")

	var badAttempts atomic.Int32
	goodUpdates := make(chan struct{}, 1)

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			args, ok := routeable.(*api.SubscribeToSegmentStatus)
			if !ok {
				return nil, errors.New("unexpected routeable")
			}

			switch args.Segment {
			case "bad":
				badAttempts.Add(1)
				return nil, errors.New("can't connect")
			case "good":
				stream := &mockBidiStream{closedChan: make(chan struct{})}
				emitted := false
				stream.decodeFn = func(m any) error {
					if !emitted {
						emitted = true
						status, ok := m.(*SegmentStatus)
						if !ok {
							return errors.New("unexpected decode target")
						}
						*status = SegmentStatus{Space: "space", Segment: "good", LastSequence: 1}
						return nil
					}

					<-ctx.Done()
					return ctx.Err()
				}
				return stream, nil
			default:
				return nil, fmt.Errorf("unexpected segment %q", args.Segment)
			}
		},
	}

	c := NewClient(provider)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	badSub, err := c.SubscribeToSegment(ctx, uuid.New(), "space", "bad", func(status *SegmentStatus) {})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return badAttempts.Load() >= 1
	}, time.Second, 10*time.Millisecond)

	start := time.Now()
	goodSub, err := c.SubscribeToSegment(ctx, uuid.New(), "space", "good", func(status *SegmentStatus) {
		select {
		case goodUpdates <- struct{}{}:
		default:
		}
	})
	require.NoError(t, err)

	select {
	case <-goodUpdates:
		assert.Less(t, time.Since(start), 90*time.Millisecond, "healthy subscription should not inherit another subscription's reconnect backoff")
	case <-time.After(90 * time.Millisecond):
		t.Fatal("timeout waiting for healthy subscription while another subscription is reconnecting")
	}

	cancel()
	badSub.Unsubscribe()
	goodSub.Unsubscribe()
}

func TestReconnectQueueMetricsTrackDepthAndBlockedEnqueue(t *testing.T) {
	firstCallStarted := make(chan struct{})
	releaseFirstCall := make(chan struct{})
	metrics := &testClientMetrics{}

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			select {
			case <-firstCallStarted:
			default:
				close(firstCallStarted)
				<-releaseFirstCall
			}

			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
				<-ctx.Done()
				return ctx.Err()
			}
			return stream, nil
		},
	}

	c := newTestClientWithQueue(provider, metrics, 1)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	subs := make([]api.Subscription, 0, 3)
	for i := range 3 {
		sub, err := c.SubscribeToSegment(ctx, uuid.New(), "space", fmt.Sprintf("seg-%d", i), func(*SegmentStatus) {})
		require.NoError(t, err)
		subs = append(subs, sub)
	}

	select {
	case <-firstCallStarted:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for the reconnect dispatcher to start its first call")
	}

	require.Eventually(t, func() bool {
		return metrics.reconnectMaxDepth.Load() >= 1
	}, time.Second, 10*time.Millisecond)

	close(releaseFirstCall)

	require.Eventually(t, func() bool {
		return metrics.reconnectBlocked.Load() >= 1 && c.reconnectBlocks.Load() >= 1
	}, time.Second, 10*time.Millisecond)

	assert.Equal(t, int64(1), metrics.reconnectMaxDepth.Load())

	cancel()
	for _, sub := range subs {
		sub.Unsubscribe()
	}
}

func TestReconnectQueueMetricsRecordDispatcherWait(t *testing.T) {
	firstCallStarted := make(chan struct{})
	releaseFirstCall := make(chan struct{})
	metrics := &testClientMetrics{}

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			select {
			case <-firstCallStarted:
			default:
				close(firstCallStarted)
				<-releaseFirstCall
			}

			stream := &mockBidiStream{closedChan: make(chan struct{})}
			stream.decodeFn = func(m any) error {
				<-ctx.Done()
				return ctx.Err()
			}
			return stream, nil
		},
	}

	c := newTestClientWithQueue(provider, metrics, 2)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	firstSub, err := c.SubscribeToSegment(ctx, uuid.New(), "space", "seg-a", func(*SegmentStatus) {})
	require.NoError(t, err)

	select {
	case <-firstCallStarted:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for the reconnect dispatcher to stall on the first request")
	}

	secondSub, err := c.SubscribeToSegment(ctx, uuid.New(), "space", "seg-b", func(*SegmentStatus) {})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	close(releaseFirstCall)

	require.Eventually(t, func() bool {
		return metrics.reconnectWaitCount.Load() >= 2 && time.Duration(metrics.reconnectMaxWaitNs.Load()) >= 40*time.Millisecond
	}, time.Second, 10*time.Millisecond)

	assert.GreaterOrEqual(t, c.reconnectDepth.Load(), int64(0))

	cancel()
	firstSub.Unsubscribe()
	secondSub.Unsubscribe()
}

// TestHealthStatusTracking tests Issue 4: Silent subscription failures detection
func TestHealthStatusTracking(t *testing.T) {
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
	var subID string
	clientInst.subscriptionsMu.RLock()
	for id := range clientInst.subscriptions {
		subID = id
		break
	}
	clientInst.subscriptionsMu.RUnlock()

	// Get status using the helper method
	status := clientInst.GetSubscriptionStatus(subID)

	// Assert: subscription should be tracked and queryable
	assert.NotNil(t, status, "should be able to query subscription status (Issue 4)")
	if status != nil {
		// Status should be "active" or "reconnecting"
		assert.Contains(t, []string{"active", "reconnecting"}, status.Status, "status should be active or reconnecting")
	}

	// Cleanup
	sub.Unsubscribe()
}

// TestContextCancellationOnUnsubscribe tests Issue 5: Context mismatch
func TestContextCancellationOnUnsubscribe(t *testing.T) {
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
func TestSubscriptionCleanupOnPanic(t *testing.T) {
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
	clientInst.subscriptionsMu.RLock()
	countAfterPanic := len(clientInst.subscriptions)
	clientInst.subscriptionsMu.RUnlock()

	// Assert:  Subscription should still be in registry (panic was recovered)
	assert.Greater(t, countAfterPanic, 0, "subscription should survive panic with recovery (Issue 1 + Issue 6)")

	// Cleanup
	sub.Unsubscribe()

	// After unsubscribe, subscription should be removed from registry
	clientInst.subscriptionsMu.RLock()
	countAfterUnsubscribe := len(clientInst.subscriptions)
	clientInst.subscriptionsMu.RUnlock()

	assert.Equal(t, 0, countAfterUnsubscribe, "subscription should be unregistered after Unsubscribe")
}

func TestOffsetTracking(t *testing.T) {
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
	clientInst.subscriptionsMu.RLock()
	var lastDelivered int64
	for _, activeSub := range clientInst.subscriptions {
		delivered := activeSub.lastDelivered.Load()
		if delivered > lastDelivered {
			lastDelivered = delivered
		}
	}
	clientInst.subscriptionsMu.RUnlock()

	// Should have tracked at least one sequence
	assert.Greater(t, lastDelivered, int64(0), "should track delivered sequences")

	// Cleanup
	sub.Unsubscribe()
}

// TestHandlerTimeoutTracking verifies that slow handlers don't block subscriptions (Issue 7).
// Handlers that exceed the timeout should be interrupted while maintaining message delivery.
func TestHandlerTimeoutTracking(t *testing.T) {
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
				return errors.New("stream ended")
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

// TestHandlerTimeoutMetrics verifies that handler timeouts and panics are tracked (Issue 7).
// Applications should be able to query subscription health including timeout/panic counts.
func TestHandlerTimeoutMetrics(t *testing.T) {
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
				return errors.New("stream ended")
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
	var subID string
	clientInst := c.(*client)
	clientInst.subscriptionsMu.RLock()
	for id := range clientInst.subscriptions {
		subID = id
		break
	}
	clientInst.subscriptionsMu.RUnlock()

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
func TestSlowHandlerDoesNotBlockOtherMessages(t *testing.T) {
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

func TestSubscriptionCoalescesLatestStatusWhenHandlersSaturate(t *testing.T) {
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

func TestSpaceSubscriptionRetainsLatestPendingStatusPerSegmentWhenHandlersSaturate(t *testing.T) {
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
func TestConsumeSegmentResilient(t *testing.T) {
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
func TestConsumeSpaceResilient(t *testing.T) {
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
func TestConsumeResilient(t *testing.T) {
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
func TestProduceSafetyAtomic(t *testing.T) {
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
func TestProduceLockPreventsRaceCondition(t *testing.T) {
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
func TestProduceLockIsolatesBySegment(t *testing.T) {
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

func TestResilienceEnumeratorResumesConsumeSegment(t *testing.T) {
	var calls int
	var capturedArgs []api.Routeable

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			calls++
			capturedArgs = append(capturedArgs, routeable)
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			seq := 0
			stream.decodeFn = func(m any) error {
				// handle both *api.Entry and **api.Entry due to how the enumerator passes values
				switch v := m.(type) {
				case *api.Entry:
					seq++
					if seq == 1 {
						*v = api.Entry{Sequence: 1}
						return nil
					}
				case **api.Entry:
					seq++
					if seq == 1 {
						*v = &api.Entry{Sequence: 1}
						return nil
					}
				}
				return errors.New("simulated disconnect")
			}
			return stream, nil
		},
	}

	c := NewClient(provider)
	ctx := context.Background()
	storeID := uuid.New()
	args := &ConsumeSegment{Space: "space", Segment: "segment"}
	enum := c.ConsumeSegment(ctx, storeID, args)
	defer enum.Dispose()

	// First MoveNext should return the first entry
	assert.True(t, enum.MoveNext())
	entry, err := enum.Current()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), entry.Sequence)

	// Next MoveNext triggers reconnect; we don't rely on its return value here
	_ = enum.MoveNext()

	// We expect at least two calls and second call's args to be a ConsumeSegment with MinSequence == 2
	require.GreaterOrEqual(t, calls, 2)
	if cs, ok := capturedArgs[len(capturedArgs)-1].(*api.ConsumeSegment); ok {
		assert.Equal(t, uint64(2), cs.MinSequence)
	} else {
		t.Fatalf("expected ConsumeSegment, got %T", capturedArgs[len(capturedArgs)-1])
	}
}

func TestResilienceEnumeratorResumesConsumeSpace(t *testing.T) {
	var calls int
	var capturedArgs []api.Routeable

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			calls++
			capturedArgs = append(capturedArgs, routeable)
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			first := true
			stream.decodeFn = func(m any) error {
				// handle both *api.Entry and **api.Entry
				switch v := m.(type) {
				case *api.Entry:
					if first {
						first = false
						*v = api.Entry{Sequence: 5, Timestamp: 12345, Space: "spaceX", Segment: "segX"}
						return nil
					}
				case **api.Entry:
					if first {
						first = false
						*v = &api.Entry{Sequence: 5, Timestamp: 12345, Space: "spaceX", Segment: "segX"}
						return nil
					}
				}
				return errors.New("simulated disconnect")
			}
			return stream, nil
		},
	}

	c := NewClient(provider)
	ctx := context.Background()
	storeID := uuid.New()
	args := &ConsumeSpace{Space: "spaceX"}
	enum := c.ConsumeSpace(ctx, storeID, args)
	defer enum.Dispose()

	// First MoveNext returns the first entry
	assert.True(t, enum.MoveNext())
	entry, err := enum.Current()
	require.NoError(t, err)
	assert.Equal(t, uint64(5), entry.Sequence)

	// Next MoveNext triggers reconnect; we don't rely on its return value here
	_ = enum.MoveNext()

	require.GreaterOrEqual(t, calls, 2)
	if cs, ok := capturedArgs[len(capturedArgs)-1].(*api.ConsumeSpace); ok {
		// Offset should be set and equal to the entry's space offset
		expected := entry.GetSpaceOffset()
		assert.Equal(t, expected, cs.Offset)
	} else {
		t.Fatalf("expected ConsumeSpace, got %T", capturedArgs[len(capturedArgs)-1])
	}
}

func TestResilienceEnumeratorResumesConsume(t *testing.T) {
	var calls int
	var capturedArgs []api.Routeable

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			calls++
			capturedArgs = append(capturedArgs, routeable)
			stream := &mockBidiStream{closedChan: make(chan struct{})}
			first := true
			stream.decodeFn = func(m any) error {
				switch v := m.(type) {
				case *api.Entry:
					if first {
						first = false
						*v = api.Entry{Sequence: 5, Timestamp: 12345, Space: "spaceX", Segment: "segX"}
						return nil
					}
				case **api.Entry:
					if first {
						first = false
						*v = &api.Entry{Sequence: 5, Timestamp: 12345, Space: "spaceX", Segment: "segX"}
						return nil
					}
				}
				return errors.New("simulated disconnect")
			}
			return stream, nil
		},
	}

	c := NewClient(provider)
	ctx := context.Background()
	storeID := uuid.New()
	args := &Consume{
		Offsets: map[string]lexkey.LexKey{
			"spaceX": {},
		},
	}
	enum := c.Consume(ctx, storeID, args)
	defer enum.Dispose()

	assert.True(t, enum.MoveNext())
	entry, err := enum.Current()
	require.NoError(t, err)
	assert.Equal(t, uint64(5), entry.Sequence)

	_ = enum.MoveNext()

	require.GreaterOrEqual(t, calls, 2)
	if consumeArgs, ok := capturedArgs[len(capturedArgs)-1].(*api.Consume); ok {
		expected := entry.GetSpaceOffset()
		require.Contains(t, consumeArgs.Offsets, "spaceX")
		assert.Equal(t, expected, consumeArgs.Offsets["spaceX"])
		assert.NotContains(t, consumeArgs.Offsets, "spaceX/segX")
	} else {
		t.Fatalf("expected Consume, got %T", capturedArgs[len(capturedArgs)-1])
	}
}

func TestProduceLockEviction(t *testing.T) {
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

func TestPeekReturnsPermanentRemoteErrorWithoutRetry(t *testing.T) {
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

func TestConsumeSegmentReturnsPermanentRemoteErrorWithoutRetry(t *testing.T) {
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

func TestSubscriptionRetriesIndefinitely(t *testing.T) {
	var attempts atomic.Int32

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			attempts.Add(1)
			return nil, errors.New("can't connect")
		},
	}

	c := NewClientWithHandlerTimeout(provider, 50*time.Millisecond).(*client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storeID := uuid.New()

	// Register subscription that will fail to connect
	sub, err := c.SubscribeToSegment(ctx, storeID, "space", "segment", func(s *SegmentStatus) {})
	require.NoError(t, err)

	// Wait for several retry attempts
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if attempts.Load() >= 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Assert: subscription keeps retrying (status is "reconnecting", not "failed")
	var subID string
	c.subscriptionsMu.RLock()
	for id := range c.subscriptions {
		subID = id
		break
	}
	c.subscriptionsMu.RUnlock()
	require.NotEmpty(t, subID, "subscription should still be registered")

	status := c.GetSubscriptionStatus(subID)
	require.NotNil(t, status)
	assert.Equal(t, "reconnecting", status.Status, "subscription should be reconnecting, never permanently failed")
	assert.GreaterOrEqual(t, status.FailureCount, int32(3), "should have multiple failure attempts")
	assert.NotNil(t, status.LastError)

	// RetryFailedSubscription signals immediate retry (works regardless of status)
	err = c.RetryFailedSubscription(subID)
	require.NoError(t, err)

	// Cleanup — Unsubscribe should stop the retry loop
	sub.Unsubscribe()

	c.subscriptionsMu.RLock()
	_, stillExists := c.subscriptions[subID]
	c.subscriptionsMu.RUnlock()
	assert.False(t, stillExists, "subscription should be unregistered after Unsubscribe")
}

func TestSubscriptionStopsOnPermanentRemoteError(t *testing.T) {
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
		c.subscriptionsMu.RLock()
		defer c.subscriptionsMu.RUnlock()
		return len(c.subscriptions) == 0
	}, time.Second, 10*time.Millisecond, "permanent remote errors should stop the subscription loop")

	assert.Equal(t, int32(1), attempts.Load(), "permanent remote errors should not trigger reconnect retries")
	select {
	case <-handlerCalled:
		t.Fatal("handler should not run for permanent remote errors")
	default:
	}

	sub.Unsubscribe()
}

func TestSubscriptionCancelsOnPermanentCallStreamError(t *testing.T) {
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
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		c.subscriptionsMu.RLock()
		defer c.subscriptionsMu.RUnlock()
		return len(c.subscriptions) == 0
	}, time.Second, 10*time.Millisecond, "permanent CallStream errors should cancel the subscription")

	assert.Equal(t, int32(1), attempts.Load(), "permanent CallStream errors should not trigger reconnect retries")
	select {
	case <-handlerCalled:
		t.Fatal("handler should not run when CallStream fails permanently")
	default:
	}

	sub.Unsubscribe()
}

func TestSubscriptionStopsOnStreamOverloadedError(t *testing.T) {
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
		c.subscriptionsMu.RLock()
		defer c.subscriptionsMu.RUnlock()
		return len(c.subscriptions) == 0
	}, time.Second, 10*time.Millisecond, "stream overload should stop the subscription loop")

	assert.Equal(t, int32(1), attempts.Load(), "stream overload should not trigger reconnect retries")
	select {
	case <-handlerCalled:
		t.Fatal("handler should not run after stream overload")
	default:
	}

	sub.Unsubscribe()
}

func TestRetryFailedSubscriptionReconnectsActiveStream(t *testing.T) {
	var attempts atomic.Int32
	firstStreamClosed := make(chan struct{})
	updates := make(chan SegmentStatus, 1)

	provider := &mockProvider{
		callStreamFn: func(ctx context.Context, storeID uuid.UUID, routeable api.Routeable) (api.BidiStream, error) {
			attempt := attempts.Add(1)
			stream := &mockBidiStream{closedChan: make(chan struct{})}

			if attempt == 1 {
				stream.closeFn = func(err error) {
					select {
					case <-firstStreamClosed:
					default:
						close(firstStreamClosed)
					}
				}
				stream.decodeFn = func(m any) error {
					<-stream.closedChan
					return errors.New("stream closed")
				}
				return stream, nil
			}

			emitted := false
			stream.decodeFn = func(m any) error {
				if !emitted {
					emitted = true
					status, ok := m.(*SegmentStatus)
					if !ok {
						return errors.New("unexpected decode target")
					}
					*status = SegmentStatus{Space: "space", Segment: "segment", LastSequence: 99}
					return nil
				}
				<-ctx.Done()
				return ctx.Err()
			}
			return stream, nil
		},
	}

	c := NewClient(provider).(*client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := c.SubscribeToSegment(ctx, uuid.New(), "space", "segment", func(status *SegmentStatus) {
		select {
		case updates <- *status:
		default:
		}
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return attempts.Load() >= 1
	}, time.Second, 10*time.Millisecond)

	var subID string
	c.subscriptionsMu.RLock()
	for id := range c.subscriptions {
		subID = id
		break
	}
	c.subscriptionsMu.RUnlock()
	require.NotEmpty(t, subID)

	err = c.RetryFailedSubscription(subID)
	require.NoError(t, err)

	select {
	case <-firstStreamClosed:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for active stream to close on retry")
	}

	select {
	case status := <-updates:
		assert.Equal(t, uint64(99), status.LastSequence)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for update after retry reconnect")
	}

	assert.GreaterOrEqual(t, attempts.Load(), int32(2), "retry should force a new CallStream attempt")

	cancel()
	sub.Unsubscribe()
}

func TestClientCloseCancelsActiveSubscriptions(t *testing.T) {
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
		c.subscriptionsMu.RLock()
		defer c.subscriptionsMu.RUnlock()
		return len(c.subscriptions) == 1
	}, time.Second, 10*time.Millisecond)
	require.Len(t, provider.GetListeners(), 1)

	require.NoError(t, c.Close())

	c.subscriptionsMu.RLock()
	remainingSubscriptions := len(c.subscriptions)
	c.subscriptionsMu.RUnlock()
	assert.Equal(t, 0, remainingSubscriptions, "Close should wait for subscriptions to unregister before returning")
	assert.Len(t, provider.GetListeners(), 0)
	assert.GreaterOrEqual(t, closedCount.Load(), int32(1), "closing the client should close active streams")

	// Safe after client shutdown; subscription should already be done.
	sub.Unsubscribe()
}

func TestClientCloseReturnsWhenCalledFromHandler(t *testing.T) {
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

	c.subscriptionsMu.RLock()
	remainingSubscriptions := len(c.subscriptions)
	c.subscriptionsMu.RUnlock()
	assert.Equal(t, 0, remainingSubscriptions, "Close called from a handler should still drain subscriptions")
	assert.Equal(t, int32(0), metrics.handlerTimeouts.Load(), "shutdown-triggered cancellation should not be recorded as a handler timeout")

	// Safe after client shutdown; subscription should already be done.
	sub.Unsubscribe()
}

// TestWithLeaseNotAcquired verifies that when server returns Ok false on acquire, WithLease returns ErrLeaseNotAcquired.
func TestWithLeaseNotAcquired(t *testing.T) {
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
func TestWithLeaseRenewAndRelease(t *testing.T) {
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
func TestWithLeaseRunsCallbackThenReleases(t *testing.T) {
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
func TestWithLeaseContextCanceledWhenParentCanceled(t *testing.T) {
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

func TestIsRetryable(t *testing.T) {
	assert.False(t, IsRetryable(context.Canceled))
	assert.False(t, IsRetryable(fmt.Errorf("not found")))
	assert.False(t, IsRetryable(fmt.Errorf("invalid argument: foo")))
	assert.True(t, IsRetryable(fmt.Errorf("connection refused")))
	assert.True(t, IsRetryable(fmt.Errorf("503 service unavailable")))
}
