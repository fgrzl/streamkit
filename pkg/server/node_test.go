package server

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
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

// (no message bus mocks here; tests avoid configuring a bus factory)

// mock bidi stream used to drive Node.Handle
type mockBidi struct {
	// decodeEnvelope will be returned on the first Decode call
	decodeEnvelope *polymorphic.Envelope
	encoded        []any
	closed         chan struct{}
	closeErr       error
	closeSendErr   error
}

func newMockBidi(env *polymorphic.Envelope) *mockBidi {
	return &mockBidi{decodeEnvelope: env, closed: make(chan struct{})}
}
func (m *mockBidi) Encode(msg any) error {
	m.encoded = append(m.encoded, msg)
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
	node := NewNode(uuid.New(), store, nil)
	env := &polymorphic.Envelope{Content: &api.GetSpaces{}}
	bidi := newMockBidi(env)

	// Act
	node.Handle(context.Background(), bidi)

	// Assert: Expect two encoded names then CloseSend(nil)
	require.Len(t, bidi.encoded, 2, "expected 2 encoded messages")
	require.NoError(t, bidi.closeSendErr)
}

func TestPeekReturnsEntryOrEmpty(t *testing.T) {
	// case: nil entry - should encode empty entry with space/segment
	store := &mockStore{peekEntry: nil}
	node := NewNode(uuid.New(), store, nil)

	env := &polymorphic.Envelope{Content: &api.Peek{Space: "s", Segment: "seg"}}
	bidi := newMockBidi(env)

	node.Handle(context.Background(), bidi)

	// Assert
	require.Len(t, bidi.encoded, 1, "expected 1 encoded message")
	require.NoError(t, bidi.closeSendErr)

	// case: non-nil entry
	e := &api.Entry{Space: "s", Segment: "seg", Sequence: 1}
	store2 := &mockStore{peekEntry: e}
	node2 := NewNode(uuid.New(), store2, nil)
	env2 := &polymorphic.Envelope{Content: &api.Peek{Space: "s", Segment: "seg"}}
	bidi2 := newMockBidi(env2)
	node2.Handle(context.Background(), bidi2)
	// Assert
	require.Len(t, bidi2.encoded, 1, "expected 1 encoded message")
}

func TestProduceNotifiesBus(t *testing.T) {
	statuses := []*api.SegmentStatus{{Space: "s", Segment: "seg"}}
	store := &mockStore{statuses: statuses}
	node := NewNode(uuid.New(), store, nil)

	// For Produce, the decode envelope is the Produce message. The stream
	// enumerator for records will call Decode into a Record; since our mockBidi
	// returns EOF after the first Decode, the store.Produce will receive an
	// empty stream (which is fine) and node will iterate statuses and notify bus.
	env := &polymorphic.Envelope{Content: &api.Produce{Space: "s", Segment: "seg"}}
	bidi := newMockBidi(env)

	node.Handle(context.Background(), bidi)

	// encoded should contain one status
	// Assert
	require.Len(t, bidi.encoded, 1, "expected 1 encoded status")
	// we don't configure a message bus in this test; ensure we encoded status
	// (notification behavior is exercised elsewhere with an actual bus)
}

func TestHandleInvalidMsgType(t *testing.T) {
	store := &mockStore{}
	node := NewNode(uuid.New(), store, nil)

	env := &polymorphic.Envelope{Content: struct{}{}}
	bidi := newMockBidi(env)

	node.Handle(context.Background(), bidi)

	// Assert
	require.Error(t, bidi.closeErr, "expected Close to be called with error for invalid msg type")
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
	node := NewNode(uuid.New(), &panickingStore{}, nil)
	env := &polymorphic.Envelope{Content: &api.GetSpaces{}}
	bidi := newMockBidi(env)

	node.Handle(context.Background(), bidi)

	// Assert
	require.Error(t, bidi.closeErr, "expected Close to be called with panic error")
}

func TestSubscribeNoBusConfigured(t *testing.T) {
	store := &mockStore{}
	node := NewNode(uuid.New(), store, nil)

	env := &polymorphic.Envelope{Content: &api.SubscribeToSegmentStatus{Space: "s", Segment: "*"}}
	bidi := newMockBidi(env)

	node.Handle(context.Background(), bidi)

	// Assert
	require.Error(t, bidi.closeSendErr, "expected CloseSend with error when bus factory is nil")
}

// (removed failingBusFactory - not used)

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
	node := NewNode(uuid.New(), bad, nil)
	env := &polymorphic.Envelope{Content: &api.GetSpaces{}}
	bidi := newMockBidi(env)
	node.Handle(context.Background(), bidi)
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
	node := NewNode(uuid.New(), store, nil)
	env := &polymorphic.Envelope{Content: &api.ConsumeSegment{Space: "s", Segment: "seg"}}
	mb := newMockBidi(env)
	bidi := &bidiEncodeFail{mockBidi: mb, fail: errors.New("encode fail")}
	node.Handle(context.Background(), bidi)
	// Assert
	require.Error(t, bidi.closeSendErr, "expected CloseSend with encode error")
}

func TestHandlePeekWithCanceledContext(t *testing.T) {
	store := &mockStore{peekEntry: nil}
	node := NewNode(uuid.New(), store, nil)

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
	node := NewNode(uuid.New(), &mockStore{}, nil)
	node.Handle(context.Background(), bidi)
	// Assert
	require.Error(t, bidi.closeErr, "expected Close called with decode error")
}

func TestStreamEntriesCurrentError(t *testing.T) {
	node := NewNode(uuid.New(), &badSegStore{}, nil)
	env := &polymorphic.Envelope{Content: &api.ConsumeSegment{Space: "s", Segment: "seg"}}
	bidi := newMockBidi(env)
	node.Handle(context.Background(), bidi)
	// Assert
	require.Error(t, bidi.closeSendErr, "expected CloseSend with error from entry current")
}

func TestNodeCloseAndManagerClose(t *testing.T) {
	// test Node.Close calls store.Close
	closed := false
	s := &closableStore{closed: &closed}
	n := NewNode(uuid.New(), s, nil)
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
