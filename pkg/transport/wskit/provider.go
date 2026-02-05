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
	// reconnect RNG for jittered backoff (protected by rngMu for thread-safety)
	rng   *rand.Rand
	rngMu sync.Mutex
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

	// reconnect listener management
	listenersMu sync.RWMutex
	listeners   []api.ReconnectListener
	// hasConnected tracks if we've ever successfully connected (vs initial connect)
	hasConnected atomic.Bool
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
// Implements resilient retry with exponential backoff for transient muxer closure.
func (p *WebSocketBidiStreamProvider) CallStream(ctx context.Context, storeID uuid.UUID, msg api.Routeable) (api.BidiStream, error) {
	// Retry loop for transient muxer closure: if muxer closes during Encode,
	// recreate the muxer and retry with exponential backoff.
	// Max attempts tuned to allow sufficient reconnection time in production.
	const maxEncodeAttempts = 7
	var lastErr error
	baseDelay := time.Duration(100) * time.Millisecond
	maxDelay := time.Duration(10) * time.Second

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
				slog.WarnContext(ctx, "provider: muxer closed during encode, retrying",
					slog.Int("attempt", attempt),
					slog.Int("maxAttempts", maxEncodeAttempts),
					slog.String("channelID", channelID.String()))

				// Force invalidate the current muxer to ensure next attempt creates fresh connection
				p.invalidateMuxer()

				// exponential backoff with jitter: baseDelay * 2^(attempt-1) +/- up to 10% jitter
				backoff := baseDelay * (1 << uint(attempt-1))
				if backoff > maxDelay {
					backoff = maxDelay
				}
				// Add jitter: +/- 10% of backoff
				jitter := time.Duration(p.randInt63n(int64(backoff/5))) * time.Nanosecond
				if p.randIntn(2) == 0 {
					backoff = backoff + jitter
				} else {
					backoff = backoff - jitter
				}

				slog.DebugContext(ctx, "provider: encode retry backoff",
					slog.Duration("backoff", backoff),
					slog.Int("attempt", attempt))

				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(backoff):
				}
				continue
			}
			// Non-muxer errors are fatal (context cancellation, auth errors, etc.)
			slog.WarnContext(ctx, "provider: encode failed with non-transient error",
				slog.String("error", err.Error()),
				slog.String("channelID", channelID.String()))
			return nil, err
		}
		return bidi, nil
	}

	if lastErr != nil {
		slog.ErrorContext(ctx, "provider: exhausted all retry attempts",
			slog.Int("maxAttempts", maxEncodeAttempts),
			slog.String("lastError", lastErr.Error()))

		// Final fallback: if all retries failed with muxer closed, attempt one final aggressive recovery
		// by forcing a complete muxer invalidation and trying once more. This handles the case where
		// the muxer becomes unhealthy right after Ping() succeeds due to race conditions.
		if errors.Is(lastErr, ErrMuxerClosed) || lastErr == ErrMuxerClosed {
			slog.WarnContext(ctx, "provider: attempting final recovery with forced muxer recreation")
			p.invalidateMuxer()

			// Give a brief moment for any pending closure to complete
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}

			// Final attempt: create completely fresh muxer
			muxer, err := p.getOrCreateMuxer(ctx)
			if err == nil {
				channelID := uuid.New()
				bidi := muxer.Register(storeID, channelID)
				envelope := polymorphic.NewEnvelope(msg)
				if err = bidi.Encode(envelope); err == nil {
					slog.InfoContext(ctx, "provider: final recovery attempt succeeded")
					return bidi, nil
				}
				bidi.Close(err)
				slog.WarnContext(ctx, "provider: final recovery attempt failed",
					slog.String("error", err.Error()),
					slog.String("channelID", channelID.String()))
			}
		}

		return nil, lastErr
	}
	return nil, errors.New("failed to call stream")
}

// randInt63n returns a thread-safe random int64 in [0,n)
func (p *WebSocketBidiStreamProvider) randInt63n(n int64) int64 {
	p.rngMu.Lock()
	defer p.rngMu.Unlock()
	return p.rng.Int63n(n)
}

// randIntn returns a thread-safe random int in [0,n)
func (p *WebSocketBidiStreamProvider) randIntn(n int) int {
	p.rngMu.Lock()
	defer p.rngMu.Unlock()
	return p.rng.Intn(n)
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

				// Coordinate with getOrCreateMuxer: if another goroutine is already dialing,
				// skip this iteration and wait. This prevents the race condition where
				// reconnect loop creates a new muxer that immediately replaces one just
				// created by getOrCreateMuxer, causing "muxer closed" errors on in-flight streams.
				if !p.dialing.CompareAndSwap(false, true) {
					slog.Debug("provider: reconnect loop skipping dial, another dial in progress")
					select {
					case <-p.reconnectCtx.Done():
						return
					case <-time.After(500 * time.Millisecond):
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

				// Release dialing lock before sleeping
				p.dialing.Store(false)

				// jittered backoff before next attempt
				select {
				case <-p.reconnectCtx.Done():
					return
				case <-time.After(backoff + time.Duration(p.randInt63n(500))*time.Millisecond):
				}
				if backoff < 30*time.Second {
					backoff *= 2
				}
			}
		}()
	})
}

// getOrCreateMuxer dials and initializes or returns the current muxer.
// Implements coordinated reconnect: if a dial is in progress, waits for completion
// rather than failing immediately, enabling resilient concurrent recovery.
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
		// Another goroutine is dialing; wait for it to complete rather than failing immediately.
		// This enables resilient concurrent reconnection - multiple goroutines can coordinate
		// recovery by waiting for the first successful dial.
		waitBackoff := 50 * time.Millisecond
		maxWaitTime := time.Duration(p.maxDialAttempts*3) * time.Second // scaled to total dial budget
		waitDeadline := time.Now().Add(maxWaitTime)

		for time.Now().Before(waitDeadline) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(waitBackoff):
				// Recheck if muxer is now available
				p.mu.Lock()
				if p.muxer != nil && p.muxer.Ping() {
					m := p.muxer
					p.mu.Unlock()
					return m, nil
				}
				p.mu.Unlock()
				// Still dialing, continue waiting with exponential backoff
				if waitBackoff < 500*time.Millisecond {
					waitBackoff *= 2
				}
			}
		}
		return nil, errors.New("dial in progress by another goroutine, timed out waiting for completion")
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

			// Check if this is a reconnect (not initial connect)
			isReconnect := p.hasConnected.Load()
			p.replaceMuxer(m)

			if isReconnect {
				slog.InfoContext(ctx, "provider: muxer recreated after reconnection")
				// Notify listeners of reconnection - use uuid.Nil to indicate all stores
				go p.notifyReconnected(p.reconnectCtx, uuid.Nil)
			} else {
				slog.InfoContext(ctx, "provider: initial muxer created")
				p.hasConnected.Store(true)
			}

			// start background reconnect loop now that we have an initial muxer
			p.startReconnectLoop()
			return m, nil
		}

		lastErr = err
		slog.WarnContext(ctx, "provider: dial attempt failed", slog.Int("attempt", attempt), slog.String("error", err.Error()))

		// compute jittered backoff: baseDelay * 2^(attempt-1) +/- 0..500ms
		backoff := baseDelay * (1 << uint(attempt-1))
		jitter := time.Duration(p.randInt63n(500)) * time.Millisecond
		// randomize add/subtract
		if p.randIntn(2) == 0 {
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
// Includes a grace period to allow in-flight streams on the old muxer to complete.
func (p *WebSocketBidiStreamProvider) replaceMuxer(newMuxer providerMuxer) {
	p.mu.Lock()
	oldMuxer := p.muxer
	p.muxer = newMuxer
	p.mu.Unlock()

	// Close old muxer outside the lock with a grace period to allow
	// in-flight operations to complete. This prevents the race where
	// a stream is registered and trying to encode just as the muxer is replaced.
	if oldMuxer != nil {
		go func(m providerMuxer) {
			// Wait for any in-flight encode operations to complete
			time.Sleep(500 * time.Millisecond)
			p.closeMuxer(m)
		}(oldMuxer)
	}
}

// invalidateMuxer clears the current muxer without replacing it,
// forcing the next getOrCreateMuxer call to dial and create a fresh connection.
// This is used when a muxer closure is detected during encode to prevent
// reusing a closed or closing muxer.
func (p *WebSocketBidiStreamProvider) invalidateMuxer() {
	p.mu.Lock()
	oldMuxer := p.muxer
	p.muxer = nil
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

// RegisterReconnectListener registers a listener to be notified when the provider reconnects.
func (p *WebSocketBidiStreamProvider) RegisterReconnectListener(listener api.ReconnectListener) {
	p.listenersMu.Lock()
	defer p.listenersMu.Unlock()
	p.listeners = append(p.listeners, listener)
}

// UnregisterReconnectListener removes a previously registered reconnect listener.
func (p *WebSocketBidiStreamProvider) UnregisterReconnectListener(listener api.ReconnectListener) {
	p.listenersMu.Lock()
	defer p.listenersMu.Unlock()
	for i, l := range p.listeners {
		if l == listener {
			p.listeners = append(p.listeners[:i], p.listeners[i+1:]...)
			break
		}
	}
}

// notifyReconnected calls all registered listeners to notify them of a successful reconnect.
// This is called when the provider establishes a new muxer connection.
func (p *WebSocketBidiStreamProvider) notifyReconnected(ctx context.Context, storeID uuid.UUID) {
	p.listenersMu.RLock()
	listeners := make([]api.ReconnectListener, len(p.listeners))
	copy(listeners, p.listeners)
	p.listenersMu.RUnlock()

	for _, listener := range listeners {
		if err := listener.OnReconnected(ctx, storeID); err != nil {
			slog.WarnContext(ctx, "reconnect listener failed", "err", err)
		}
	}
}
