package server

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streamkit/internal/lease"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/bus"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// sliceEnumerator is a tiny enumerator used in tests.
type sliceEnumerator[T any] struct {
	items []T
	idx   int
}

func (s *sliceEnumerator[T]) MoveNext() bool {
	if s.idx >= len(s.items) {
		return false
	}
	s.idx++
	return true
}

func (s *sliceEnumerator[T]) Current() (T, error) {
	var zero T
	if s.idx == 0 || s.idx > len(s.items) {
		return zero, io.EOF
	}
	return s.items[s.idx-1], nil
}

func (s *sliceEnumerator[T]) Dispose() {}

func (s *sliceEnumerator[T]) Err() error { return nil }

// mockStore implements storage.Store for tests.
type mockStore struct {
	spaces    []string
	segments  []string
	entries   []*api.Entry
	peekEntry *api.Entry
	statuses  []*api.SegmentStatus
}

func (m *mockStore) GetSpaces(ctx context.Context) enumerators.Enumerator[string] {
	return &sliceEnumerator[string]{items: m.spaces}
}
func (m *mockStore) ConsumeSpace(ctx context.Context, args *api.ConsumeSpace) enumerators.Enumerator[*api.Entry] {
	return &sliceEnumerator[*api.Entry]{items: m.entries}
}
func (m *mockStore) GetSegments(ctx context.Context, space string) enumerators.Enumerator[string] {
	if len(m.segments) > 0 {
		return &sliceEnumerator[string]{items: m.segments}
	}
	return &sliceEnumerator[string]{items: m.spaces}
}
func (m *mockStore) ConsumeSegment(ctx context.Context, args *api.ConsumeSegment) enumerators.Enumerator[*api.Entry] {
	return &sliceEnumerator[*api.Entry]{items: m.entries}
}
func (m *mockStore) Peek(ctx context.Context, space, segment string) (*api.Entry, error) {
	return m.peekEntry, nil
}
func (m *mockStore) Produce(ctx context.Context, args *api.Produce, entries enumerators.Enumerator[*api.Record]) enumerators.Enumerator[*api.SegmentStatus] {
	return &sliceEnumerator[*api.SegmentStatus]{items: m.statuses}
}
func (m *mockStore) Close() {}

type mockBusSubscription struct {
	unsubscribed atomic.Bool
	unsubCalls   atomic.Int32
}

func (s *mockBusSubscription) Unsubscribe() error {
	s.unsubscribed.Store(true)
	s.unsubCalls.Add(1)
	return nil
}

type mockMessageBus struct {
	route        bus.Route
	handler      bus.MessageHandler
	subscription *mockBusSubscription
	notifyErr    error
	notifyCalls  atomic.Int32
}

func (b *mockMessageBus) Notify(msg bus.Message) error {
	b.notifyCalls.Add(1)
	return b.notifyErr
}

func (b *mockMessageBus) NotifyWithContext(ctx context.Context, msg bus.Message) error {
	return nil
}

func (b *mockMessageBus) Subscribe(route bus.Route, handler bus.MessageHandler) (bus.Subscription, error) {
	b.route = route
	b.handler = handler
	if b.subscription == nil {
		b.subscription = &mockBusSubscription{}
	}
	return b.subscription, nil
}

func (b *mockMessageBus) Close() error {
	return nil
}

type mockMessageBusFactory struct {
	bus bus.MessageBus
	err error
}

func (f *mockMessageBusFactory) Get(ctx context.Context) (bus.MessageBus, error) {
	return f.bus, f.err
}

// mock bidi stream used to drive Node.Handle
type mockBidi struct {
	// decodeEnvelope will be returned on the first Decode call
	decodeEnvelope *polymorphic.Envelope
	encoded        []any
	encodedCh      chan any
	encodeErr      error
	closed         chan struct{}
	closeErr       error
	closeSendErr   error
}

func newMockBidi(env *polymorphic.Envelope) *mockBidi {
	return &mockBidi{decodeEnvelope: env, closed: make(chan struct{})}
}

func waitForClosed(t *testing.T, bidi api.BidiStream) {
	t.Helper()
	select {
	case <-bidi.Closed():
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for bidi stream to close")
	}
}

func (m *mockBidi) Encode(msg any) error {
	if m.encodeErr != nil {
		return m.encodeErr
	}
	m.encoded = append(m.encoded, msg)
	if m.encodedCh != nil {
		select {
		case m.encodedCh <- msg:
		default:
		}
	}
	return nil
}
func (m *mockBidi) Decode(v any) error {
	switch v := v.(type) {
	case *polymorphic.Envelope:
		if m.decodeEnvelope == nil {
			return io.EOF
		}
		*v = *m.decodeEnvelope
		// make subsequent decodes return EOF
		m.decodeEnvelope = nil
		return nil
	default:
		return errors.New("unsupported decode target")
	}
}
func (m *mockBidi) CloseSend(err error) error {
	m.closeSendErr = err
	select {
	case <-m.closed:
	default:
		close(m.closed)
	}
	return nil
}
func (m *mockBidi) Close(err error) {
	m.closeErr = err
	select {
	case <-m.closed:
	default:
		close(m.closed)
	}
}
func (m *mockBidi) EndOfStreamError() error { return io.EOF }
func (m *mockBidi) Closed() <-chan struct{} { return m.closed }

func TestGetSpacesStreamsNames(t *testing.T) {
	// Arrange
	store := &mockStore{spaces: []string{"one", "two"}}
	node := NewNode(uuid.New(), store, nil, lease.NewStore())
	env := &polymorphic.Envelope{Content: &api.GetSpaces{}}
	bidi := newMockBidi(env)

	// Act
	node.Handle(context.Background(), bidi)
	waitForClosed(t, bidi)

	// Assert: Expect two encoded names then CloseSend(nil)
	require.Len(t, bidi.encoded, 2, "expected 2 encoded messages")
	require.NoError(t, bidi.closeSendErr)
}

func TestPeekReturnsEntryOrEmpty(t *testing.T) {
	// case: nil entry - should encode empty entry with space/segment
	store := &mockStore{peekEntry: nil}
	node := NewNode(uuid.New(), store, nil, lease.NewStore())

	env := &polymorphic.Envelope{Content: &api.Peek{Space: "s", Segment: "seg"}}
	bidi := newMockBidi(env)

	node.Handle(context.Background(), bidi)

	// Assert
	require.Len(t, bidi.encoded, 1, "expected 1 encoded message")
	require.NoError(t, bidi.closeSendErr)

	// case: non-nil entry
	e := &api.Entry{Space: "s", Segment: "seg", Sequence: 1}
	store2 := &mockStore{peekEntry: e}
	node2 := NewNode(uuid.New(), store2, nil, lease.NewStore())
	env2 := &polymorphic.Envelope{Content: &api.Peek{Space: "s", Segment: "seg"}}
	bidi2 := newMockBidi(env2)
	node2.Handle(context.Background(), bidi2)
	waitForClosed(t, bidi2)
	// Assert
	require.Len(t, bidi2.encoded, 1, "expected 1 encoded message")
}

func TestProduceNotifiesBus(t *testing.T) {
	statuses := []*api.SegmentStatus{{Space: "s", Segment: "seg"}}
	store := &mockStore{statuses: statuses}
	node := NewNode(uuid.New(), store, nil, lease.NewStore())

	// For Produce, the decode envelope is the Produce message. The stream
	// enumerator for records will call Decode into a Record; since our mockBidi
	// returns EOF after the first Decode, the store.Produce will receive an
	// empty stream (which is fine) and node will iterate statuses and notify bus.
	env := &polymorphic.Envelope{Content: &api.Produce{Space: "s", Segment: "seg"}}
	bidi := newMockBidi(env)

	node.Handle(context.Background(), bidi)
	waitForClosed(t, bidi)

	// encoded should contain one status
	// Assert
	require.Len(t, bidi.encoded, 1, "expected 1 encoded status")
	// we don't configure a message bus in this test; ensure we encoded status
	// (notification behavior is exercised elsewhere with an actual bus)
}

func TestProduceOpensNotificationCircuitAfterRepeatedBusFailures(t *testing.T) {
	statuses := []*api.SegmentStatus{{Space: "s", Segment: "seg", LastSequence: 1}}
	store := &mockStore{statuses: statuses}
	messageBus := &mockMessageBus{notifyErr: errors.New("notify failed")}
	node := NewNode(uuid.New(), store, &mockMessageBusFactory{bus: messageBus}, lease.NewStore()).(*defaultNode)
	node.notifyFailureThreshold = 2
	node.notifyCircuitOpenWindow = time.Hour

	first := newMockBidi(&polymorphic.Envelope{Content: &api.Produce{Space: "s", Segment: "seg"}})
	node.Handle(context.Background(), first)
	waitForClosed(t, first)
	require.NoError(t, first.closeSendErr)
	require.Equal(t, int32(1), messageBus.notifyCalls.Load())

	second := newMockBidi(&polymorphic.Envelope{Content: &api.Produce{Space: "s", Segment: "seg"}})
	node.Handle(context.Background(), second)
	waitForClosed(t, second)
	require.ErrorContains(t, second.closeSendErr, "segment notification circuit open after 2 consecutive failures")
	require.Equal(t, int32(2), messageBus.notifyCalls.Load())

	messageBus.notifyErr = nil
	third := newMockBidi(&polymorphic.Envelope{Content: &api.Produce{Space: "s", Segment: "seg"}})
	node.Handle(context.Background(), third)
	waitForClosed(t, third)
	require.ErrorContains(t, third.closeSendErr, "segment notification circuit open")
	require.Equal(t, int32(2), messageBus.notifyCalls.Load(), "expected circuit-open produce to fail fast without notify call")

	node.notifyMu.Lock()
	node.notifyCircuitOpenUntil = time.Now().Add(-time.Second)
	node.notifyMu.Unlock()

	fourth := newMockBidi(&polymorphic.Envelope{Content: &api.Produce{Space: "s", Segment: "seg"}})
	node.Handle(context.Background(), fourth)
	waitForClosed(t, fourth)
	require.NoError(t, fourth.closeSendErr)
	require.Equal(t, int32(3), messageBus.notifyCalls.Load())
	require.Equal(t, 0, node.notifyFailureCount)
}

func TestHandleInvalidMsgType(t *testing.T) {
	store := &mockStore{}
	node := NewNode(uuid.New(), store, nil, lease.NewStore())

	env := &polymorphic.Envelope{Content: struct{}{}}
	bidi := newMockBidi(env)

	node.Handle(context.Background(), bidi)

	// Assert
	require.Error(t, bidi.closeErr, "expected Close to be called with error for invalid msg type")
}

func TestLeaseAcquireReturnsOk(t *testing.T) {
	store := &mockStore{}
	node := NewNode(uuid.New(), store, nil, lease.NewStore())
	env := &polymorphic.Envelope{Content: &api.LeaseAcquire{Key: "k", Holder: "h1", TTLSeconds: 30}}
	bidi := newMockBidi(env)

	node.Handle(context.Background(), bidi)

	require.Len(t, bidi.encoded, 1)
	result, ok := bidi.encoded[0].(*api.LeaseResult)
	require.True(t, ok)
	require.True(t, result.Ok)
}

func TestLeaseAcquireConflictReturnsNotOk(t *testing.T) {
	leaseStore := lease.NewStore()
	_ = leaseStore.Acquire("k", "h0", 30*time.Second)
	store := &mockStore{}
	node := NewNode(uuid.New(), store, nil, leaseStore)
	env := &polymorphic.Envelope{Content: &api.LeaseAcquire{Key: "k", Holder: "h0", TTLSeconds: 30}}
	bidi := newMockBidi(env)
	node.Handle(context.Background(), bidi)
	require.Len(t, bidi.encoded, 1)
	require.True(t, bidi.encoded[0].(*api.LeaseResult).Ok)

	// second holder fails
	env2 := &polymorphic.Envelope{Content: &api.LeaseAcquire{Key: "k", Holder: "h1", TTLSeconds: 30}}
	bidi2 := newMockBidi(env2)
	node.Handle(context.Background(), bidi2)
	require.Len(t, bidi2.encoded, 1)
	require.False(t, bidi2.encoded[0].(*api.LeaseResult).Ok)
}

func TestLeaseRenewAndReleaseReturnOk(t *testing.T) {
	leaseStore := lease.NewStore()
	_ = leaseStore.Acquire("k", "h1", 30*time.Second)
	store := &mockStore{}
	node := NewNode(uuid.New(), store, nil, leaseStore)

	envRenew := &polymorphic.Envelope{Content: &api.LeaseRenew{Key: "k", Holder: "h1", TTLSeconds: 60}}
	bidiRenew := newMockBidi(envRenew)
	node.Handle(context.Background(), bidiRenew)
	require.Len(t, bidiRenew.encoded, 1)
	require.True(t, bidiRenew.encoded[0].(*api.LeaseResult).Ok)

	envRelease := &polymorphic.Envelope{Content: &api.LeaseRelease{Key: "k", Holder: "h1"}}
	bidiRelease := newMockBidi(envRelease)
	node.Handle(context.Background(), bidiRelease)
	require.Len(t, bidiRelease.encoded, 1)
	require.True(t, bidiRelease.encoded[0].(*api.LeaseResult).Ok)
}

func TestLeaseAcquireValidationFailures(t *testing.T) {
	store := &mockStore{}
	node := NewNode(uuid.New(), store, nil, lease.NewStore())

	tests := []struct {
		name    string
		args    *api.LeaseAcquire
		message string
	}{
		{
			name:    "empty key",
			args:    &api.LeaseAcquire{Key: "", Holder: "h1", TTLSeconds: 30},
			message: "lease key must not be empty",
		},
		{
			name:    "empty holder",
			args:    &api.LeaseAcquire{Key: "k", Holder: "", TTLSeconds: 30},
			message: "lease holder must not be empty",
		},
		{
			name:    "ttl above max",
			args:    &api.LeaseAcquire{Key: "k", Holder: "h1", TTLSeconds: 86401},
			message: "ttl_seconds exceeds maximum of 86400",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := &polymorphic.Envelope{Content: tt.args}
			bidi := newMockBidi(env)
			node.Handle(context.Background(), bidi)
			require.Len(t, bidi.encoded, 1)
			result, ok := bidi.encoded[0].(*api.LeaseResult)
			require.True(t, ok)
			require.False(t, result.Ok)
			require.Equal(t, tt.message, result.Message)
		})
	}
}

func TestLeaseReleaseValidationFailures(t *testing.T) {
	store := &mockStore{}
	node := NewNode(uuid.New(), store, nil, lease.NewStore())

	env := &polymorphic.Envelope{Content: &api.LeaseRelease{Key: "", Holder: "h1"}}
	bidi := newMockBidi(env)
	node.Handle(context.Background(), bidi)
	require.Len(t, bidi.encoded, 1)
	result, ok := bidi.encoded[0].(*api.LeaseResult)
	require.True(t, ok)
	require.False(t, result.Ok)
	require.Equal(t, "lease key must not be empty", result.Message)
}

type panickingStore struct{}

func (p *panickingStore) GetSpaces(ctx context.Context) enumerators.Enumerator[string] { panic("boom") }
func (p *panickingStore) ConsumeSpace(ctx context.Context, args *api.ConsumeSpace) enumerators.Enumerator[*api.Entry] {
	return &sliceEnumerator[*api.Entry]{}
}
func (p *panickingStore) GetSegments(ctx context.Context, space string) enumerators.Enumerator[string] {
	return &sliceEnumerator[string]{}
}
func (p *panickingStore) ConsumeSegment(ctx context.Context, args *api.ConsumeSegment) enumerators.Enumerator[*api.Entry] {
	return &sliceEnumerator[*api.Entry]{}
}
func (p *panickingStore) Peek(ctx context.Context, space, segment string) (*api.Entry, error) {
	return nil, nil
}
func (p *panickingStore) Produce(ctx context.Context, args *api.Produce, entries enumerators.Enumerator[*api.Record]) enumerators.Enumerator[*api.SegmentStatus] {
	return &sliceEnumerator[*api.SegmentStatus]{}
}
func (p *panickingStore) Close() {}

func TestHandlePanicRecovered(t *testing.T) {
	node := NewNode(uuid.New(), &panickingStore{}, nil, lease.NewStore())
	env := &polymorphic.Envelope{Content: &api.GetSpaces{}}
	bidi := newMockBidi(env)

	node.Handle(context.Background(), bidi)

	// Assert
	require.Error(t, bidi.closeErr, "expected Close to be called with panic error")
}

func TestSubscribeNoBusConfigured(t *testing.T) {
	store := &mockStore{}
	node := NewNode(uuid.New(), store, nil, lease.NewStore())

	env := &polymorphic.Envelope{Content: &api.SubscribeToSegmentStatus{Space: "s", Segment: "*"}}
	bidi := newMockBidi(env)

	node.Handle(context.Background(), bidi)

	// Assert
	require.Error(t, bidi.closeSendErr, "expected CloseSend with error when bus factory is nil")
}

func TestSubscribeEmitsHeartbeats(t *testing.T) {
	store := &mockStore{}
	messageBus := &mockMessageBus{}
	node := NewNode(uuid.New(), store, &mockMessageBusFactory{bus: messageBus}, lease.NewStore())

	ctx, cancel := context.WithCancel(context.Background())
	env := &polymorphic.Envelope{Content: &api.SubscribeToSegmentStatus{
		Space:                    "s",
		Segment:                  "*",
		HeartbeatIntervalSeconds: 1,
	}}
	bidi := newMockBidi(env)
	bidi.encodedCh = make(chan any, 4)

	node.Handle(ctx, bidi)

	select {
	case msg := <-bidi.encodedCh:
		status, ok := msg.(*api.SegmentStatus)
		require.True(t, ok, "expected SegmentStatus heartbeat")
		require.True(t, status.Heartbeat)
		require.Equal(t, "s", status.Space)
		require.Equal(t, "*", status.Segment)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for subscription heartbeat")
	}

	cancel()
	waitForClosed(t, bidi)
	require.NotNil(t, messageBus.subscription)
	require.True(t, messageBus.subscription.unsubscribed.Load())
	require.Equal(t, int32(1), messageBus.subscription.unsubCalls.Load())
}

func TestSubscribeSendsInitialSegmentSnapshot(t *testing.T) {
	store := &mockStore{
		entries: []*api.Entry{
			{Space: "s", Segment: "seg", Sequence: 1, Timestamp: 100},
		},
		peekEntry: &api.Entry{Space: "s", Segment: "seg", Sequence: 3, Timestamp: 300},
	}
	messageBus := &mockMessageBus{}
	node := NewNode(uuid.New(), store, &mockMessageBusFactory{bus: messageBus}, lease.NewStore())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bidi := newMockBidi(&polymorphic.Envelope{Content: &api.SubscribeToSegmentStatus{
		Space:   "s",
		Segment: "seg",
	}})
	bidi.encodedCh = make(chan any, 4)

	node.Handle(ctx, bidi)

	select {
	case msg := <-bidi.encodedCh:
		status, ok := msg.(*api.SegmentStatus)
		require.True(t, ok, "expected initial SegmentStatus snapshot")
		require.False(t, status.Heartbeat)
		require.Equal(t, "s", status.Space)
		require.Equal(t, "seg", status.Segment)
		require.Equal(t, uint64(1), status.FirstSequence)
		require.Equal(t, int64(100), status.FirstTimestamp)
		require.Equal(t, uint64(3), status.LastSequence)
		require.Equal(t, int64(300), status.LastTimestamp)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for subscription snapshot")
	}

	cancel()
	waitForClosed(t, bidi)
}

func TestSubscribeSendsInitialSpaceSnapshots(t *testing.T) {
	store := &spaceSnapshotStore{
		segments: []string{"seg-b", "seg-a"},
		first: map[string]*api.Entry{
			"seg-a": {Space: "s", Segment: "seg-a", Sequence: 1, Timestamp: 100},
			"seg-b": {Space: "s", Segment: "seg-b", Sequence: 5, Timestamp: 500},
		},
		last: map[string]*api.Entry{
			"seg-a": {Space: "s", Segment: "seg-a", Sequence: 2, Timestamp: 200},
			"seg-b": {Space: "s", Segment: "seg-b", Sequence: 7, Timestamp: 700},
		},
	}
	messageBus := &mockMessageBus{}
	node := NewNode(uuid.New(), store, &mockMessageBusFactory{bus: messageBus}, lease.NewStore())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bidi := newMockBidi(&polymorphic.Envelope{Content: &api.SubscribeToSegmentStatus{
		Space:   "s",
		Segment: "*",
	}})
	bidi.encodedCh = make(chan any, 8)

	node.Handle(ctx, bidi)

	var snapshots []*api.SegmentStatus
	require.Eventually(t, func() bool {
		for len(bidi.encodedCh) > 0 {
			msg := <-bidi.encodedCh
			status, ok := msg.(*api.SegmentStatus)
			if ok && !status.Heartbeat {
				snapshots = append(snapshots, status)
			}
		}
		return len(snapshots) == 2
	}, time.Second, 10*time.Millisecond)

	require.Len(t, snapshots, 2)
	require.Equal(t, "seg-a", snapshots[0].Segment)
	require.Equal(t, uint64(2), snapshots[0].LastSequence)
	require.Equal(t, "seg-b", snapshots[1].Segment)
	require.Equal(t, uint64(7), snapshots[1].LastSequence)

	cancel()
	waitForClosed(t, bidi)
}

func TestSubscribeBuffersNotificationsUntilSnapshotCompletes(t *testing.T) {
	store := &blockingSpaceSnapshotStore{
		spaceSnapshotStore: &spaceSnapshotStore{
			segments: []string{"seg-b", "seg-a"},
			first: map[string]*api.Entry{
				"seg-a": {Space: "s", Segment: "seg-a", Sequence: 1, Timestamp: 100},
				"seg-b": {Space: "s", Segment: "seg-b", Sequence: 3, Timestamp: 300},
			},
			last: map[string]*api.Entry{
				"seg-a": {Space: "s", Segment: "seg-a", Sequence: 2, Timestamp: 200},
				"seg-b": {Space: "s", Segment: "seg-b", Sequence: 4, Timestamp: 400},
			},
		},
		blockSegment: "seg-a",
		blockStarted: make(chan struct{}),
		releaseBlock: make(chan struct{}),
	}
	messageBus := &mockMessageBus{}
	storeID := uuid.New()
	node := NewNode(storeID, store, &mockMessageBusFactory{bus: messageBus}, lease.NewStore())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bidi := newMockBidi(&polymorphic.Envelope{Content: &api.SubscribeToSegmentStatus{
		Space:   "s",
		Segment: "*",
	}})
	bidi.encodedCh = make(chan any, 8)

	handleDone := make(chan struct{})
	go func() {
		node.Handle(ctx, bidi)
		close(handleDone)
	}()

	select {
	case <-store.blockStarted:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for snapshot collection to block")
	}

	require.NotNil(t, messageBus.handler)
	require.NoError(t, messageBus.handler(context.Background(), &api.SegmentNotification{
		StoreID: storeID,
		SegmentStatus: &api.SegmentStatus{
			Space:         "s",
			Segment:       "seg-a",
			LastSequence:  6,
			LastTimestamp: 600,
		},
	}))
	require.NoError(t, messageBus.handler(context.Background(), &api.SegmentNotification{
		StoreID: storeID,
		SegmentStatus: &api.SegmentStatus{
			Space:         "s",
			Segment:       "seg-a",
			LastSequence:  7,
			LastTimestamp: 700,
		},
	}))
	require.NoError(t, messageBus.handler(context.Background(), &api.SegmentNotification{
		StoreID: storeID,
		SegmentStatus: &api.SegmentStatus{
			Space:         "s",
			Segment:       "seg-b",
			LastSequence:  9,
			LastTimestamp: 900,
		},
	}))

	close(store.releaseBlock)

	select {
	case <-handleDone:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for subscribe handler to return")
	}

	var statuses []*api.SegmentStatus
	require.Eventually(t, func() bool {
		for len(statuses) < 4 {
			select {
			case msg := <-bidi.encodedCh:
				status, ok := msg.(*api.SegmentStatus)
				if ok && !status.Heartbeat {
					statuses = append(statuses, status)
				}
			default:
				return false
			}
		}
		return true
	}, time.Second, 10*time.Millisecond)

	require.Len(t, statuses, 4)
	require.Equal(t, "seg-a", statuses[0].Segment)
	require.Equal(t, uint64(2), statuses[0].LastSequence)
	require.Equal(t, "seg-b", statuses[1].Segment)
	require.Equal(t, uint64(4), statuses[1].LastSequence)
	require.Equal(t, "seg-a", statuses[2].Segment)
	require.Equal(t, uint64(7), statuses[2].LastSequence)
	require.Equal(t, "seg-b", statuses[3].Segment)
	require.Equal(t, uint64(9), statuses[3].LastSequence)
	for _, status := range statuses {
		require.Falsef(t, status.Segment == "seg-a" && status.LastSequence == 6, "older buffered state should be replaced")
	}

	cancel()
	waitForClosed(t, bidi)
}

func TestSubscribeClosesWhenInitialHeartbeatFails(t *testing.T) {
	store := &mockStore{}
	messageBus := &mockMessageBus{}
	node := NewNode(uuid.New(), store, &mockMessageBusFactory{bus: messageBus}, lease.NewStore())

	bidi := newMockBidi(&polymorphic.Envelope{Content: &api.SubscribeToSegmentStatus{
		Space:                    "s",
		Segment:                  "*",
		HeartbeatIntervalSeconds: 1,
	}})
	bidi.encodeErr = errors.New("encode failed")

	node.Handle(context.Background(), bidi)

	waitForClosed(t, bidi)
	require.ErrorContains(t, bidi.closeErr, "encode failed")
	require.Eventually(t, func() bool {
		return messageBus.subscription != nil && messageBus.subscription.unsubscribed.Load()
	}, time.Second, 10*time.Millisecond)
	require.NotNil(t, messageBus.subscription)
	require.Equal(t, int32(1), messageBus.subscription.unsubCalls.Load())
}

func TestClampSubscriptionHeartbeatIntervalSeconds(t *testing.T) {
	assert.Equal(t, int64(0), clampSubscriptionHeartbeatIntervalSeconds(0))
	assert.Equal(t, int64(1), clampSubscriptionHeartbeatIntervalSeconds(1))
	assert.Equal(t, int64(maxSubscriptionHeartbeatIntervalSeconds), clampSubscriptionHeartbeatIntervalSeconds(3600))
}

// (removed failingBusFactory - not used)

type spaceSnapshotStore struct {
	segments []string
	first    map[string]*api.Entry
	last     map[string]*api.Entry
}

func (s *spaceSnapshotStore) GetSpaces(context.Context) enumerators.Enumerator[string] {
	return &sliceEnumerator[string]{items: []string{"s"}}
}

func (s *spaceSnapshotStore) ConsumeSpace(context.Context, *api.ConsumeSpace) enumerators.Enumerator[*api.Entry] {
	return &sliceEnumerator[*api.Entry]{}
}

func (s *spaceSnapshotStore) GetSegments(context.Context, string) enumerators.Enumerator[string] {
	return &sliceEnumerator[string]{items: s.segments}
}

func (s *spaceSnapshotStore) ConsumeSegment(ctx context.Context, args *api.ConsumeSegment) enumerators.Enumerator[*api.Entry] {
	entry := s.first[args.Segment]
	if entry == nil {
		return &sliceEnumerator[*api.Entry]{}
	}
	return &sliceEnumerator[*api.Entry]{items: []*api.Entry{entry}}
}

func (s *spaceSnapshotStore) Peek(ctx context.Context, space, segment string) (*api.Entry, error) {
	return s.last[segment], nil
}

func (s *spaceSnapshotStore) Produce(context.Context, *api.Produce, enumerators.Enumerator[*api.Record]) enumerators.Enumerator[*api.SegmentStatus] {
	return &sliceEnumerator[*api.SegmentStatus]{}
}

func (s *spaceSnapshotStore) Close() {}

type blockingSpaceSnapshotStore struct {
	*spaceSnapshotStore
	blockSegment string
	blockStarted chan struct{}
	releaseBlock chan struct{}
	blockOnce    sync.Once
}

func (s *blockingSpaceSnapshotStore) Peek(ctx context.Context, space, segment string) (*api.Entry, error) {
	if segment == s.blockSegment {
		s.blockOnce.Do(func() {
			close(s.blockStarted)
			select {
			case <-s.releaseBlock:
			case <-ctx.Done():
			}
		})
	}
	return s.spaceSnapshotStore.Peek(ctx, space, segment)
}

// errorEnumerator returns an error from Current()
type errorEnumerator[T any] struct {
	called bool
	err    error
}

func (e *errorEnumerator[T]) MoveNext() bool {
	if e.called {
		return false
	}
	e.called = true
	return true
}
func (e *errorEnumerator[T]) Current() (T, error) { var zero T; return zero, e.err }
func (e *errorEnumerator[T]) Dispose()            {}
func (e *errorEnumerator[T]) Err() error          { return e.err }

func TestStreamNamesCurrentError(t *testing.T) {
	// override GetSpaces to return an error enumerator
	storeGetter := func(ctx context.Context) enumerators.Enumerator[string] {
		return &errorEnumerator[string]{err: errors.New("bad current")}
	}
	bad := &badStore{getter: storeGetter}
	node := NewNode(uuid.New(), bad, nil, lease.NewStore())
	env := &polymorphic.Envelope{Content: &api.GetSpaces{}}
	bidi := newMockBidi(env)
	node.Handle(context.Background(), bidi)
	waitForClosed(t, bidi)
	// Assert
	require.Error(t, bidi.closeSendErr, "expected CloseSend with error from enumerator.Current")
}

// badStore is a minimal storage.Store implementation used by tests to
// inject a custom GetSpaces enumerator.
type badStore struct {
	getter func(context.Context) enumerators.Enumerator[string]
}

func (b *badStore) GetSpaces(ctx context.Context) enumerators.Enumerator[string] {
	return b.getter(ctx)
}
func (b *badStore) ConsumeSpace(ctx context.Context, args *api.ConsumeSpace) enumerators.Enumerator[*api.Entry] {
	return &sliceEnumerator[*api.Entry]{}
}
func (b *badStore) GetSegments(ctx context.Context, space string) enumerators.Enumerator[string] {
	return &sliceEnumerator[string]{}
}
func (b *badStore) ConsumeSegment(ctx context.Context, args *api.ConsumeSegment) enumerators.Enumerator[*api.Entry] {
	return &sliceEnumerator[*api.Entry]{}
}
func (b *badStore) Peek(ctx context.Context, space, segment string) (*api.Entry, error) {
	return nil, nil
}
func (b *badStore) Produce(ctx context.Context, args *api.Produce, entries enumerators.Enumerator[*api.Record]) enumerators.Enumerator[*api.SegmentStatus] {
	return &sliceEnumerator[*api.SegmentStatus]{}
}
func (b *badStore) Close() {}

// badBidi returns an error on Decode
type badBidi struct {
	*mockBidi
	decErr error
}

func (b *badBidi) Decode(v any) error { return b.decErr }

// badSegStore returns an enumerator that errors for ConsumeSegment
type badSegStore struct{}

func (b *badSegStore) GetSpaces(ctx context.Context) enumerators.Enumerator[string] {
	return &sliceEnumerator[string]{}
}
func (b *badSegStore) ConsumeSpace(ctx context.Context, args *api.ConsumeSpace) enumerators.Enumerator[*api.Entry] {
	return &sliceEnumerator[*api.Entry]{}
}
func (b *badSegStore) GetSegments(ctx context.Context, space string) enumerators.Enumerator[string] {
	return &sliceEnumerator[string]{}
}
func (b *badSegStore) ConsumeSegment(ctx context.Context, args *api.ConsumeSegment) enumerators.Enumerator[*api.Entry] {
	return &errorEnumerator[*api.Entry]{err: errors.New("bad current")}
}
func (b *badSegStore) Peek(ctx context.Context, space, segment string) (*api.Entry, error) {
	return nil, nil
}
func (b *badSegStore) Produce(ctx context.Context, args *api.Produce, entries enumerators.Enumerator[*api.Record]) enumerators.Enumerator[*api.SegmentStatus] {
	return &sliceEnumerator[*api.SegmentStatus]{}
}
func (b *badSegStore) Close() {}

// closableStore flips the provided flag when Close is called
type closableStore struct{ closed *bool }

func (c *closableStore) GetSpaces(ctx context.Context) enumerators.Enumerator[string] {
	return &sliceEnumerator[string]{}
}
func (c *closableStore) ConsumeSpace(ctx context.Context, args *api.ConsumeSpace) enumerators.Enumerator[*api.Entry] {
	return &sliceEnumerator[*api.Entry]{}
}
func (c *closableStore) GetSegments(ctx context.Context, space string) enumerators.Enumerator[string] {
	return &sliceEnumerator[string]{}
}
func (c *closableStore) ConsumeSegment(ctx context.Context, args *api.ConsumeSegment) enumerators.Enumerator[*api.Entry] {
	return &sliceEnumerator[*api.Entry]{}
}
func (c *closableStore) Peek(ctx context.Context, space, segment string) (*api.Entry, error) {
	return nil, nil
}
func (c *closableStore) Produce(ctx context.Context, args *api.Produce, entries enumerators.Enumerator[*api.Record]) enumerators.Enumerator[*api.SegmentStatus] {
	return &sliceEnumerator[*api.SegmentStatus]{}
}
func (c *closableStore) Close() {
	if c.closed != nil {
		*c.closed = true
	}
}

// bidi that fails on Encode
type bidiEncodeFail struct {
	*mockBidi
	fail error
}

func (b *bidiEncodeFail) Encode(m any) error { return b.fail }

func TestStreamEntriesEncodeError(t *testing.T) {
	store := &mockStore{entries: []*api.Entry{{Space: "s", Segment: "seg"}}}
	node := NewNode(uuid.New(), store, nil, lease.NewStore())
	env := &polymorphic.Envelope{Content: &api.ConsumeSegment{Space: "s", Segment: "seg"}}
	mb := newMockBidi(env)
	bidi := &bidiEncodeFail{mockBidi: mb, fail: errors.New("encode fail")}
	node.Handle(context.Background(), bidi)
	waitForClosed(t, bidi)
	// Assert
	require.Error(t, bidi.closeSendErr, "expected CloseSend with encode error")
}

func TestHandlePeekWithCanceledContext(t *testing.T) {
	store := &mockStore{peekEntry: nil}
	node := NewNode(uuid.New(), store, nil, lease.NewStore())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	env := &polymorphic.Envelope{Content: &api.Peek{Space: "s", Segment: "seg"}}
	bidi := newMockBidi(env)
	node.Handle(ctx, bidi)
	// Assert
	require.Error(t, bidi.closeSendErr, "expected CloseSend with context canceled error")
}

func TestHandleDecodeErrorClosesStream(t *testing.T) {
	mb := &mockBidi{closed: make(chan struct{})}
	bidi := &badBidi{mockBidi: mb, decErr: errors.New("decode failed")}
	node := NewNode(uuid.New(), &mockStore{}, nil, lease.NewStore())
	node.Handle(context.Background(), bidi)
	// Assert
	require.Error(t, bidi.closeErr, "expected Close called with decode error")
}

func TestStreamEntriesCurrentError(t *testing.T) {
	node := NewNode(uuid.New(), &badSegStore{}, nil, lease.NewStore())
	env := &polymorphic.Envelope{Content: &api.ConsumeSegment{Space: "s", Segment: "seg"}}
	bidi := newMockBidi(env)
	node.Handle(context.Background(), bidi)
	waitForClosed(t, bidi)
	// Assert
	require.Error(t, bidi.closeSendErr, "expected CloseSend with error from entry current")
}

type blockingEntryEnumerator struct {
	release <-chan struct{}
	once    bool
}

func (e *blockingEntryEnumerator) MoveNext() bool {
	if e.once {
		return false
	}
	e.once = true
	<-e.release
	return false
}

func (e *blockingEntryEnumerator) Current() (*api.Entry, error) { return nil, nil }
func (e *blockingEntryEnumerator) Dispose()                     {}
func (e *blockingEntryEnumerator) Err() error                   { return nil }

type blockingConsumeStore struct {
	release <-chan struct{}
}

func (b *blockingConsumeStore) GetSpaces(ctx context.Context) enumerators.Enumerator[string] {
	return &sliceEnumerator[string]{}
}
func (b *blockingConsumeStore) ConsumeSpace(ctx context.Context, args *api.ConsumeSpace) enumerators.Enumerator[*api.Entry] {
	return &sliceEnumerator[*api.Entry]{}
}
func (b *blockingConsumeStore) GetSegments(ctx context.Context, space string) enumerators.Enumerator[string] {
	return &sliceEnumerator[string]{}
}
func (b *blockingConsumeStore) ConsumeSegment(ctx context.Context, args *api.ConsumeSegment) enumerators.Enumerator[*api.Entry] {
	return &blockingEntryEnumerator{release: b.release}
}
func (b *blockingConsumeStore) Peek(ctx context.Context, space, segment string) (*api.Entry, error) {
	return nil, nil
}
func (b *blockingConsumeStore) Produce(ctx context.Context, args *api.Produce, entries enumerators.Enumerator[*api.Record]) enumerators.Enumerator[*api.SegmentStatus] {
	return &sliceEnumerator[*api.SegmentStatus]{}
}
func (b *blockingConsumeStore) Close() {}

func TestShouldEndConsumeSegmentSpanBeforeStreamingCompletes(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
	})

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter)),
	)
	otel.SetTracerProvider(tp)
	defer tp.Shutdown(context.Background())

	release := make(chan struct{})
	store := &blockingConsumeStore{release: release}
	node := NewNode(uuid.New(), store, nil, lease.NewStore())
	env := &polymorphic.Envelope{Content: &api.ConsumeSegment{Space: "s", Segment: "seg"}}
	bidi := newMockBidi(env)

	node.Handle(context.Background(), bidi)

	require.NoError(t, tp.ForceFlush(context.Background()))
	spans := exporter.GetSpans()
	found := false
	for i := range spans {
		if spans[i].Name == "streamkit.server.consume_segment" {
			found = true
			break
		}
	}
	require.True(t, found, "expected consume segment span to end before streaming completes")

	close(release)
	waitForClosed(t, bidi)
}

func TestNodeCloseAndManagerClose(t *testing.T) {
	// test Node.Close calls store.Close
	closed := false
	s := &closableStore{closed: &closed}
	n := NewNode(uuid.New(), s, nil, lease.NewStore())
	n.Close()
	// Assert
	require.True(t, closed, "expected store Close to be called")

	// test NodeManager.Close iterates nodes and closes them
	sf := &testStoreFactory{}
	m := NewNodeManager(WithStoreFactory(sf))
	id := uuid.New()
	n2, err := m.GetOrCreate(context.Background(), id)
	require.NoError(t, err)
	// close should not panic
	m.Close()
	// after close, further Close calls should be no-op
	m.Close()
	_ = n2
}
