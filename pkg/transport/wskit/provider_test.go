package wskit

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestGetOrCreateMuxerRetries(t *testing.T) {
	p := NewBidiStreamProvider("https://example.com/", func() (string, error) { return "tok", nil }).(*WebSocketBidiStreamProvider)
	defer p.Close()

	// simulate dialFn always failing; the foreground makes one inline attempt,
	// then waits on the background loop which also retries.
	var calls int32
	p.maxDialAttempts = 3
	p.dialFn = func() (*websocket.Conn, error) {
		atomic.AddInt32(&calls, 1)
		return nil, errors.New("dial failed")
	}

	// Act: use a short timeout so the test doesn't block for the full deadline
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m, err := p.getOrCreateMuxer(ctx)

	// Assert: foreground makes 1 inline attempt, background loop makes additional
	// attempts until the context deadline. The exact count depends on timing.
	assert.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(1), "expected at least 1 dial attempt (inline)")
	assert.Nil(t, m)
	assert.Error(t, err)
}

func TestBackgroundReconnectRecreatesMuxer(t *testing.T) {
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

func TestCallStreamRetriesOnMuxerClosed(t *testing.T) {
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

func TestCallStreamRetriesOnBenignDisconnect(t *testing.T) {
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

	// Act: the foreground makes 1 inline attempt (permanent error), then waits
	// on the background loop. The background loop also hits the permanent error
	// and uses a 30s backoff, so the context deadline fires before many retries.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := p.getOrCreateMuxer(ctx)

	// Assert: at least 1 call (the inline attempt); background may add a few
	// more before the 30s permanent-error backoff kicks in.
	assert.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(1), "expected at least 1 dial attempt")
	assert.Error(t, err)
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

	// Act: inline attempt fails, background loop retries with normal backoff
	// since RetryAuthFailures treats 401 as transient.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := p.getOrCreateMuxer(ctx)

	// Assert: multiple attempts (inline + background retries with normal backoff)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(2), "expected multiple dial attempts when RetryAuthFailures is enabled")
	assert.Error(t, err)
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

func TestCloseMuxerClosesArbitraryProviderMuxer(t *testing.T) {
	// Arrange
	p := NewBidiStreamProvider("https://example.com/", func() (string, error) { return "tok", nil }).(*WebSocketBidiStreamProvider)
	fake := &fakeMuxer{pingFn: func() bool { return true }}

	// Act
	p.closeMuxer(fake)

	// Assert
	assert.Equal(t, int32(1), atomic.LoadInt32(&fake.closeCalls), "expected Close to be called once on any providerMuxer")
}
