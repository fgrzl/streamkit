package wskit

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
	"golang.org/x/net/websocket"
)

// small provider muxer interface to allow test fakes
type providerMuxer interface {
	Ping() bool
	Register(uuid.UUID, uuid.UUID) api.BidiStream
}

// WebSocketBidiStreamProvider manages a per-tenant WebSocket connection and muxer.
type WebSocketBidiStreamProvider struct {
	addr     string
	origin   string
	fetchJWT func() (string, error)

	mu      sync.Mutex
	muxer   providerMuxer
	dialing atomic.Bool // prevents concurrent dial storms
	// reconnect RNG for jittered backoff
	rng *rand.Rand
	// testable hooks / configuration
	dialFn          func() (*websocket.Conn, error)
	maxDialAttempts int
	// injectable muxer factory for easier testing
	newClientMuxer func(ctx context.Context, session MuxerSession, conn *websocket.Conn) providerMuxer

	// background reconnect management
	reconnectOnce   sync.Once
	reconnectCtx    context.Context
	reconnectCancel context.CancelFunc
	stopped         atomic.Bool
}

// NewBidiStreamProvider creates a provider that uses a dedicated WebSocket connection per client.
func NewBidiStreamProvider(addr string, fetchJWT func() (string, error)) api.BidiStreamProvider {

	// normalize address: ensure it ends with /streamz and avoid duplicate slashes
	// trim any trailing slashes, then append the segment
	a := strings.TrimRight(addr, "/")
	a = a + "/streamz"

	p := &WebSocketBidiStreamProvider{
		addr:     a,
		origin:   "http://localhost",
		fetchJWT: fetchJWT,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
		// sensible defaults; tests may override
		dialFn:          nil,
		maxDialAttempts: 5,
	}
	// default muxer factory uses real client muxer
	p.newClientMuxer = func(ctx context.Context, session MuxerSession, conn *websocket.Conn) providerMuxer {
		return NewClientWebSocketMuxer(ctx, session, conn)
	}
	p.reconnectCtx, p.reconnectCancel = context.WithCancel(context.Background())
	return p
}

// CallStream opens or reuses a muxed stream over the WebSocket for a single logical interaction.
func (p *WebSocketBidiStreamProvider) CallStream(ctx context.Context, storeID uuid.UUID, msg api.Routeable) (api.BidiStream, error) {
	// Retry loop for transient muxer closure: if muxer closes during Encode,
	// recreate the muxer and retry up to a bounded number of attempts.
	const maxEncodeAttempts = 3
	var lastErr error

	for attempt := 1; attempt <= maxEncodeAttempts; attempt++ {
		muxer, err := p.getOrCreateMuxer(ctx)
		if err != nil {
			return nil, err
		}
		channelID := uuid.New()
		bidi := muxer.Register(storeID, channelID)

		// we use a polymorphic envelope so that we can
		// unmarshal server side and route the msg
		envelope := polymorphic.NewEnvelope(msg)
		if err := bidi.Encode(envelope); err != nil {
			bidi.Close(err)
			lastErr = err
			// If muxer was closed, attempt reconnect and retry
			if errors.Is(err, ErrMuxerClosed) || err == ErrMuxerClosed {
				slog.WarnContext(ctx, "provider: muxer closed during encode, retrying", slog.Int("attempt", attempt))
				// small jittered backoff before retrying
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
				}
				continue
			}
			// Non-muxer errors are fatal
			return nil, err
		}
		return bidi, nil
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errors.New("failed to call stream")
}

// startReconnectLoop ensures a background goroutine keeps a healthy muxer alive.
func (p *WebSocketBidiStreamProvider) startReconnectLoop() {
	p.reconnectOnce.Do(func() {
		go func() {
			backoff := time.Second
			for {
				if p.stopped.Load() {
					return
				}
				// atomically check health while holding lock to prevent TOCTOU
				p.mu.Lock()
				m := p.muxer
				healthy := m != nil && m.Ping()
				p.mu.Unlock()

				if healthy {
					// sleep a short time before next healthcheck
					select {
					case <-p.reconnectCtx.Done():
						return
					case <-time.After(1 * time.Second):
					}
					continue
				}

				slog.Debug("provider: reconnect loop attempting dial", slog.Duration("backoff", backoff))

				// attempt to dial and create a new muxer
				var conn *websocket.Conn
				var err error
				if p.dialFn != nil {
					conn, err = p.dialFn()
				} else {
					conn, err = p.dial()
				}
				if err == nil {
					m2 := p.newClientMuxer(context.Background(), NewClientMuxerSession(), conn)
					if m2 != nil {
						p.replaceMuxer(m2)
						slog.Info("provider: reconnect successful")
						// reset backoff
						backoff = time.Second
					}
				} else {
					slog.Warn("provider: reconnect dial failed", slog.String("error", err.Error()))
				}

				// jittered backoff before next attempt
				select {
				case <-p.reconnectCtx.Done():
					return
				case <-time.After(backoff + time.Duration(p.rng.Int63n(500))*time.Millisecond):
				}
				if backoff < 30*time.Second {
					backoff *= 2
				}
			}
		}()
	})
}

// getOrCreateMuxer dials and initializes or returns the current muxer.
func (p *WebSocketBidiStreamProvider) getOrCreateMuxer(ctx context.Context) (providerMuxer, error) {
	p.mu.Lock()
	// If existing muxer is healthy, reuse it
	if p.muxer != nil && p.muxer.Ping() {
		m := p.muxer
		p.mu.Unlock()
		return m, nil
	}
	p.mu.Unlock()

	// Prevent concurrent dial storms: only one goroutine dials at a time
	if !p.dialing.CompareAndSwap(false, true) {
		// Another goroutine is dialing; wait briefly and recheck
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
			// Recheck if muxer is now available
			p.mu.Lock()
			if p.muxer != nil && p.muxer.Ping() {
				m := p.muxer
				p.mu.Unlock()
				return m, nil
			}
			p.mu.Unlock()
			return nil, errors.New("dial in progress by another goroutine")
		}
	}
	defer p.dialing.Store(false)

	// immediate connect attempts for caller (keep previous getOrCreate behavior)
	var (
		maxAttempts = p.maxDialAttempts
		baseDelay   = time.Second // initial backoff
	)

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		var conn *websocket.Conn
		var err error
		if p.dialFn != nil {
			conn, err = p.dialFn()
		} else {
			conn, err = p.dial()
		}
		if err == nil {
			m := p.newClientMuxer(ctx, NewClientMuxerSession(), conn)
			p.replaceMuxer(m)
			slog.InfoContext(ctx, "provider: initial muxer created")
			// start background reconnect loop now that we have an initial muxer
			p.startReconnectLoop()
			return m, nil
		}

		lastErr = err
		slog.WarnContext(ctx, "provider: dial attempt failed", slog.Int("attempt", attempt), slog.String("error", err.Error()))

		// compute jittered backoff: baseDelay * 2^(attempt-1) +/- 0..500ms
		backoff := baseDelay * (1 << uint(attempt-1))
		jitter := time.Duration(p.rng.Int63n(500)) * time.Millisecond
		// randomize add/subtract
		if p.rng.Intn(2) == 0 {
			backoff = backoff + jitter
		} else {
			if backoff > jitter {
				backoff = backoff - jitter
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
	}

	return nil, lastErr
}

// replaceMuxer safely replaces the current muxer, closing the old one to prevent leaks.
func (p *WebSocketBidiStreamProvider) replaceMuxer(newMuxer providerMuxer) {
	p.mu.Lock()
	oldMuxer := p.muxer
	p.muxer = newMuxer
	p.mu.Unlock()

	// Close old muxer outside the lock to prevent deadlock
	if oldMuxer != nil {
		p.closeMuxer(oldMuxer)
	}
}

// closeMuxer attempts to close a muxer if it's a *WebSocketMuxer.
func (p *WebSocketBidiStreamProvider) closeMuxer(m providerMuxer) {
	if wsMuxer, ok := m.(*WebSocketMuxer); ok {
		wsMuxer.shutdown(nil)
	}
}

// Close stops the provider's background reconnect loop and closes the active muxer.
// It is safe to call multiple times.
func (p *WebSocketBidiStreamProvider) Close() error {
	if p.stopped.Swap(true) {
		return nil
	}

	// Cancel reconnect context
	if p.reconnectCancel != nil {
		p.reconnectCancel()
	}

	// Close active muxer
	p.mu.Lock()
	currentMuxer := p.muxer
	p.muxer = nil
	p.mu.Unlock()

	if currentMuxer != nil {
		p.closeMuxer(currentMuxer)
		slog.Debug("provider: closed active muxer")
	}

	return nil
}

// dial establishes the raw WebSocket connection with token-based auth.
func (p *WebSocketBidiStreamProvider) dial() (*websocket.Conn, error) {
	cfg, err := websocket.NewConfig(p.addr, p.origin)
	if err != nil {
		return nil, err
	}

	token, err := p.fetchJWT()
	if err != nil {
		return nil, err
	}

	cfg.Header = http.Header{}
	cfg.Header.Set("Authorization", "Bearer "+token)

	return websocket.DialConfig(cfg)
}
