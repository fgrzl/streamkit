package wskit

import (
	"context"
	"errors"
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

	// simulate dialFn always failing to ensure backoff and attempts are exercised
	calls := 0
	p.maxDialAttempts = 3
	p.dialFn = func() (*websocket.Conn, error) {
		calls++
		return nil, errors.New("dial failed")
	}

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m, err := p.getOrCreateMuxer(ctx)

	// Assert
	assert.Equal(t, 3, calls, "expected dialFn to be called until max attempts")
	assert.Nil(t, m)
	assert.Error(t, err)
}

func TestBackgroundReconnectRecreatesMuxer(t *testing.T) {
	// Arrange
	p := NewBidiStreamProvider("https://example.com/", func() (string, error) { return "tok", nil }).(*WebSocketBidiStreamProvider)

	// prepare two fake muxers to be returned in sequence
	calls := 0
	f1 := &struct{ ping bool }{ping: true}
	f2 := &struct{ ping bool }{ping: true}

	p.dialFn = func() (*websocket.Conn, error) {
		return nil, nil // conn is ignored by our fake factory
	}
	p.newClientMuxer = func(ctx context.Context, session MuxerSession, conn *websocket.Conn) providerMuxer {
		calls++
		if calls == 1 {
			return &struct{ providerMuxer }{providerMuxer: &fakeMuxer{pingFn: func() bool { return f1.ping }}}
		}
		return &struct{ providerMuxer }{providerMuxer: &fakeMuxer{pingFn: func() bool { return f2.ping }}}
	}

	// Act: create initial muxer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m1, err := p.getOrCreateMuxer(ctx)
	require.NoError(t, err)
	require.NotNil(t, m1)

	// Simulate first muxer becoming unhealthy
	f1.ping = false

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
		select {
		case <-time.After(100 * time.Millisecond):
		}
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
	var failingEncodeCount int
	failing := &testBidi{
		encodeFn: func(m any) error {
			failingEncodeCount++
			p.mu.Lock()
			p.muxer = nil
			p.mu.Unlock()
			return ErrMuxerClosed
		},
	}

	// succeeding bidi: Encode succeeds
	succeeded := &testBidi{}
	encodeSucceeded := false
	succeeded.encodeFn = func(m any) error {
		encodeSucceeded = true
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
	assert.True(t, encodeSucceeded, "expected Encode to succeed after reconnect")
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

// fakeMuxer implements providerMuxer for tests
type fakeMuxer struct {
	pingFn func() bool
	bidi   api.BidiStream
}

func (f *fakeMuxer) Ping() bool                                   { return f.pingFn() }
func (f *fakeMuxer) Register(uuid.UUID, uuid.UUID) api.BidiStream { return f.bidi }
