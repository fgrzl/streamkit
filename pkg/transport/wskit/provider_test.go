package wskit

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/telemetry"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	"golang.org/x/net/websocket"
)

func TestShouldNormalizeAddressWhenCreatingProvider(t *testing.T) {
	// Arrange
	p := NewBidiStreamProvider("https://example.com/", func() (string, error) { return "tok", nil })

	// Act
	wp, ok := p.(*WebSocketBidiStreamProvider)

	// Assert
	require.True(t, ok, "expected provider to be WebSocketBidiStreamProvider")
	assert.Equal(t, "https://example.com/streamz", wp.addr)
}

func TestShouldGetOrCreateMuxerRetries(t *testing.T) {
	p := NewBidiStreamProvider("https://example.com/", func() (string, error) { return "tok", nil }).(*WebSocketBidiStreamProvider)
	defer p.Close()

	// simulate dialFn always failing; the foreground should surface the inline
	// error immediately instead of masking it behind a timeout.
	var calls int32
	p.maxDialAttempts = 3
	p.dialFn = func() (*websocket.Conn, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("dial failed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	m, err := p.getOrCreateMuxer(ctx)

	// Assert
	assert.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(1), "expected at least 1 dial attempt (inline)")
	assert.Nil(t, m)
	require.Error(t, err)
	assert.ErrorContains(t, err, "dial failed")
}

func TestShouldBackgroundReconnectRecreatesMuxer(t *testing.T) {
	// Arrange
	p := NewBidiStreamProvider("https://example.com/", func() (string, error) { return "tok", nil }).(*WebSocketBidiStreamProvider)

	// prepare two fake muxers to be returned in sequence
	var calls int32
	var f1Healthy atomic.Bool
	f1Healthy.Store(true)
	var f2Healthy atomic.Bool
	f2Healthy.Store(true)

	p.dialFn = func() (*websocket.Conn, error) {
		return nil, nil // conn is ignored by our fake factory
	}
	p.newClientMuxer = func(ctx context.Context, session MuxerSession, conn *websocket.Conn) providerMuxer {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			return &struct{ providerMuxer }{providerMuxer: &fakeMuxer{pingFn: func() bool { return f1Healthy.Load() }}}
		}
		return &struct{ providerMuxer }{providerMuxer: &fakeMuxer{pingFn: func() bool { return f2Healthy.Load() }}}
	}

	// Act: create initial muxer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m1, err := p.getOrCreateMuxer(ctx)
	require.NoError(t, err)
	require.NotNil(t, m1)

	// Simulate first muxer becoming unhealthy
	f1Healthy.Store(false)

	// Wait for background reconnect to produce the second muxer
	ok := false
	for i := 0; i < 50; i++ {
		p.mu.Lock()
		current := p.muxer
		p.mu.Unlock()
		if current != nil && current != m1 {
			ok = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Assert
	assert.True(t, ok, "expected provider to replace muxer in background")
	// Clean up
	_ = p.Close()
}

func TestShouldCallStreamRetriesOnMuxerClosed(t *testing.T) {
	// Arrange
	p := NewBidiStreamProvider("https://example.com/", func() (string, error) { return "tok", nil }).(*WebSocketBidiStreamProvider)
	defer p.Close()

	// failing bidi: Encode returns ErrMuxerClosed and clears provider muxer to force reconnect
	var failingEncodeCount int32
	failing := &testBidi{
		encodeFn: func(m any) error {
			atomic.AddInt32(&failingEncodeCount, 1)
			p.mu.Lock()
			p.muxer = nil
			p.mu.Unlock()
			return ErrMuxerClosed
		},
	}

	// succeeding bidi: Encode succeeds
	var encodeSucceeded atomic.Bool
	succeeded := &testBidi{}
	succeeded.encodeFn = func(m any) error {
		encodeSucceeded.Store(true)
		return nil
	}

	failingMux := &fakeMuxer{pingFn: func() bool { return true }, bidi: failing}
	succeedingMux := &fakeMuxer{pingFn: func() bool { return true }, bidi: succeeded}

	// initial provider has failing muxer
	p.mu.Lock()
	p.muxer = failingMux
	p.mu.Unlock()

	// make dial/newClientMuxer produce the succeeding muxer
	p.dialFn = func() (*websocket.Conn, error) { return nil, nil }
	p.newClientMuxer = func(ctx context.Context, session MuxerSession, conn *websocket.Conn) providerMuxer {
		return succeedingMux
	}

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	b, err := p.CallStream(ctx, uuid.New(), &api.GetStatus{})

	// Assert
	require.NoError(t, err)
	// returned bidi should be the succeeding one (its Encode already ran)
	assert.True(t, encodeSucceeded.Load(), "expected Encode to succeed after reconnect")
	_ = b.Close
}

func TestShouldCallStreamRetriesOnBenignDisconnect(t *testing.T) {
	// Arrange
	p := NewBidiStreamProvider("https://example.com/", func() (string, error) { return "tok", nil }).(*WebSocketBidiStreamProvider)
	defer p.Close()

	var failingEncodeCount int32
	failing := &testBidi{
		encodeFn: func(m any) error {
			atomic.AddInt32(&failingEncodeCount, 1)
			p.mu.Lock()
			p.muxer = nil
			p.mu.Unlock()
			return io.EOF
		},
	}

	var encodeSucceeded atomic.Bool
	succeeded := &testBidi{}
	succeeded.encodeFn = func(m any) error {
		encodeSucceeded.Store(true)
		return nil
	}

	failingMux := &fakeMuxer{pingFn: func() bool { return true }, bidi: failing}
	succeedingMux := &fakeMuxer{pingFn: func() bool { return true }, bidi: succeeded}

	p.mu.Lock()
	p.muxer = failingMux
	p.mu.Unlock()

	p.dialFn = func() (*websocket.Conn, error) { return nil, nil }
	p.newClientMuxer = func(ctx context.Context, session MuxerSession, conn *websocket.Conn) providerMuxer {
		return succeedingMux
	}

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	b, err := p.CallStream(ctx, uuid.New(), &api.GetStatus{})

	// Assert
	require.NoError(t, err)
	assert.True(t, encodeSucceeded.Load(), "expected Encode to succeed after benign disconnect reconnect")
	_ = b.Close
}

func TestShouldDetachCallStreamSpanFromCaller(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
	})

	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(trace.NewSimpleSpanProcessor(exporter)))
	otel.SetTracerProvider(tp)
	defer tp.Shutdown(context.Background())

	p := NewBidiStreamProvider("https://example.com/", func() (string, error) { return "tok", nil }).(*WebSocketBidiStreamProvider)
	defer p.Close()

	capture := &capturingMuxer{
		bidi: &testBidi{
			encodeFn: func(any) error { return nil },
		},
	}
	p.dialFn = func() (*websocket.Conn, error) { return nil, nil }
	p.newClientMuxer = func(ctx context.Context, session MuxerSession, conn *websocket.Conn) providerMuxer {
		return capture
	}

	requestID := uuid.New()
	tracer := telemetry.GetTracer()
	parentCtx := telemetry.WithRequestIDContext(context.Background(), requestID)
	parentCtx, parentSpan := tracer.Start(parentCtx, "caller")

	for i := 0; i < 2; i++ {
		bidi, err := p.CallStream(parentCtx, uuid.New(), &api.GetStatus{})
		require.NoError(t, err)
		if bidi != nil {
			bidi.Close(nil)
		}
	}
	parentSpan.End()

	require.NoError(t, tp.ForceFlush(context.Background()))
	spans := exporter.GetSpans()

	var callerSpan *tracetest.SpanStub
	transportSpans := make([]*tracetest.SpanStub, 0, 2)
	for i := range spans {
		s := &spans[i]
		switch s.Name {
		case "caller":
			callerSpan = s
		case "streamkit.transport.call_stream":
			transportSpans = append(transportSpans, s)
		}
	}

	require.NotNil(t, callerSpan, "expected parent span to be exported")
	require.Len(t, transportSpans, 2, "expected one transport span per CallStream invocation")

	parentTraceID := callerSpan.SpanContext.TraceID()
	seenTransportTraceIDs := make(map[string]struct{}, len(transportSpans))
	for _, transportSpan := range transportSpans {
		assert.NotEqual(t, parentTraceID, transportSpan.SpanContext.TraceID(), "transport span should start a new trace")
		require.Len(t, transportSpan.Links, 1, "transport span should keep a link to the caller span")
		assert.Equal(t, parentTraceID, transportSpan.Links[0].SpanContext.TraceID())
		seenTransportTraceIDs[transportSpan.SpanContext.TraceID().String()] = struct{}{}
	}
	assert.Len(t, seenTransportTraceIDs, 2, "each CallStream should create its own transport trace")

	require.Len(t, capture.contexts, 2)
	for _, capturedCtx := range capture.contexts {
		gotRequestID, ok := telemetry.RequestIDFromContext(capturedCtx)
		require.True(t, ok, "expected request ID to survive detachment")
		assert.Equal(t, requestID, gotRequestID)
		assert.True(t, oteltrace.SpanFromContext(capturedCtx).SpanContext().IsValid())
		assert.NotEqual(t, parentTraceID, oteltrace.SpanFromContext(capturedCtx).SpanContext().TraceID())
	}
}

// testBidi implements api.BidiStream for tests
type testBidi struct {
	encodeFn func(any) error
}

func (t *testBidi) Encode(m any) error        { return t.encodeFn(m) }
func (t *testBidi) Decode(m any) error        { return nil }
func (t *testBidi) CloseSend(err error) error { return nil }
func (t *testBidi) Close(err error)           {}
func (t *testBidi) EndOfStreamError() error   { return nil }
func (t *testBidi) Closed() <-chan struct{}   { c := make(chan struct{}); close(c); return c }

func TestShouldTreatAuthErrorsAsPermanentByDefault(t *testing.T) {
	// Arrange
	p := NewBidiStreamProvider("https://example.com/", func() (string, error) { return "tok", nil }).(*WebSocketBidiStreamProvider)
	defer p.Close()

	var calls int32
	p.maxDialAttempts = 5
	p.dialFn = func() (*websocket.Conn, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("dial error: 401 unauthorized")
	}

	// Act: the foreground should return the permanent auth error immediately.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := p.getOrCreateMuxer(ctx)

	// Assert
	assert.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(1), "expected at least 1 dial attempt")
	require.Error(t, err)
	assert.ErrorContains(t, err, "401 unauthorized")
}

func TestShouldRetryAuthErrorsWhenRetryAuthFailuresEnabled(t *testing.T) {
	// Arrange
	p := NewBidiStreamProvider("https://example.com/", func() (string, error) { return "tok", nil }).(*WebSocketBidiStreamProvider)
	defer p.Close()
	p.RetryAuthFailures = true

	var calls int32
	p.maxDialAttempts = 3
	p.dialFn = func() (*websocket.Conn, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("dial error: 401 unauthorized")
	}

	// Act: inline attempt still returns immediately, but the background loop keeps
	// retrying with normal backoff because 401 is treated as transient.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := p.getOrCreateMuxer(ctx)

	require.Error(t, err)
	assert.ErrorContains(t, err, "401 unauthorized")
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&calls) >= 2
	}, 3*time.Second, 50*time.Millisecond, "expected background retries when RetryAuthFailures is enabled")
}

func TestShouldGetOrCreateMuxerInvokesOnDialFailureForInlineErrors(t *testing.T) {
	p := NewBidiStreamProvider("https://example.com/", func() (string, error) { return "tok", nil }).(*WebSocketBidiStreamProvider)
	defer p.Close()

	var callbackCalls atomic.Int32
	p.OnDialFailure = func(err error) {
		if err != nil {
			callbackCalls.Add(1)
		}
	}
	p.dialFn = func() (*websocket.Conn, error) {
		return nil, errors.New("dial failed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := p.getOrCreateMuxer(ctx)

	require.Error(t, err)
	require.Eventually(t, func() bool {
		return callbackCalls.Load() >= 1
	}, time.Second, 10*time.Millisecond)
}

func TestShouldExtractRouteableStreamDetails(t *testing.T) {
	tests := []struct {
		name    string
		msg     api.Routeable
		op      string
		space   string
		segment string
	}{
		{
			name:    "consume segment",
			msg:     &api.ConsumeSegment{Space: "orders", Segment: "s0"},
			op:      "consume_segment",
			space:   "orders",
			segment: "s0",
		},
		{
			name:    "subscribe segment status",
			msg:     &api.SubscribeToSegmentStatus{Space: "orders", Segment: "*"},
			op:      "subscribe_to_segment_status",
			space:   "orders",
			segment: "*",
		},
		{
			name:    "get status",
			msg:     &api.GetStatus{},
			op:      "get_status",
			space:   "",
			segment: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, space, segment := routeableStreamDetails(tt.msg)
			assert.Equal(t, tt.op, op)
			assert.Equal(t, tt.space, space)
			assert.Equal(t, tt.segment, segment)
		})
	}
}

// fakeMuxer implements providerMuxer for tests
type fakeMuxer struct {
	pingFn     func() bool
	bidi       api.BidiStream
	closeCalls int32
}

func (f *fakeMuxer) Ping() bool                                   { return f.pingFn() }
func (f *fakeMuxer) Register(uuid.UUID, uuid.UUID) api.BidiStream { return f.bidi }
func (f *fakeMuxer) RegisterWithContext(context.Context, uuid.UUID, uuid.UUID) api.BidiStream {
	return f.bidi
}
func (f *fakeMuxer) Close(_ error) { atomic.AddInt32(&f.closeCalls, 1) }

type capturingMuxer struct {
	bidi     api.BidiStream
	contexts []context.Context
}

func (m *capturingMuxer) Ping() bool                                   { return true }
func (m *capturingMuxer) Register(uuid.UUID, uuid.UUID) api.BidiStream { return m.bidi }
func (m *capturingMuxer) RegisterWithContext(ctx context.Context, _, _ uuid.UUID) api.BidiStream {
	m.contexts = append(m.contexts, ctx)
	return m.bidi
}
func (m *capturingMuxer) Close(_ error) {}

func TestShouldCloseMuxerClosesArbitraryProviderMuxer(t *testing.T) {
	// Arrange
	p := NewBidiStreamProvider("https://example.com/", func() (string, error) { return "tok", nil }).(*WebSocketBidiStreamProvider)
	fake := &fakeMuxer{pingFn: func() bool { return true }}

	// Act
	p.closeMuxer(fake)

	// Assert
	assert.Equal(t, int32(1), atomic.LoadInt32(&fake.closeCalls), "expected Close to be called once on any providerMuxer")
}
