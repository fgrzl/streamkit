package wskit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/telemetry"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/net/websocket"
)

// small provider muxer interface to allow test fakes
type providerMuxer interface {
	Ping() bool
	Register(uuid.UUID, uuid.UUID) api.BidiStream
	RegisterWithContext(context.Context, uuid.UUID, uuid.UUID) api.BidiStream
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
	// RetryAuthFailures controls whether 401/auth errors are retried.
	// When true, auth failures are treated as transient (useful during JWKS propagation).
	// When false (default), auth failures are treated as permanent and fail fast.
	RetryAuthFailures bool

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

	// OnDialFailure, when set, is called whenever a dial fails (before retrying or returning).
	// Applications can use it to invalidate a token cache so the next fetchJWT returns a refreshed token.
	OnDialFailure func(err error)
}

// NewBidiStreamProvider creates a provider that uses a dedicated WebSocket connection per client.
// fetchJWT is invoked on every dial attempt and should return a fresh or refreshed token when possible.
// If the server rejects the token (e.g. 401 Unauthorized), the same token must not be returned on the
// next attempt—refresh or clear the application's token cache so the next fetchJWT returns a valid token.
// Optionally set OnDialFailure on the returned provider to be notified when dial fails (e.g. to invalidate cache).
//
// By default, 401 auth errors are treated as permanent and fail fast. To handle JWKS propagation delays
// where tokens may be temporarily rejected during key rotation, set RetryAuthFailures to true on the
// returned provider:
//
//	provider := wskit.NewBidiStreamProvider(addr, fetchJWT).(*wskit.WebSocketBidiStreamProvider)
//	provider.RetryAuthFailures = true
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
	tracer := telemetry.GetTracer()
	ctx, span := tracer.Start(ctx, "streamkit.transport.call_stream",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			telemetry.WithTransportType("websocket"),
		))
	defer span.End()

	// Retry loop for transient muxer closure: if muxer closes during Encode,
	// recreate the muxer and retry with exponential backoff.
	// Max attempts tuned to allow sufficient reconnection time in production.
	const maxEncodeAttempts = 7
	var lastErr error
	baseDelay := time.Duration(100) * time.Millisecond
	maxDelay := time.Duration(10) * time.Second

	for attempt := 1; attempt <= maxEncodeAttempts; attempt++ {
		span.SetAttributes(telemetry.WithAttempt(attempt))
		muxer, err := p.getOrCreateMuxer(ctx)
		if err != nil {
			lastErr = err
			slog.WarnContext(ctx, "provider: failed to get or create muxer, retrying",
				slog.String("store_id", storeID.String()),
				slog.Int("attempt", attempt),
				slog.Int("max_attempts", maxEncodeAttempts),
				slog.String("error_type", classifyTransportError(err)),
				slog.String("error", err.Error()))

			// Check context before backoff
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			// Backoff before retrying muxer creation
			backoff := baseDelay * (1 << uint(attempt-1))
			if backoff > maxDelay {
				backoff = maxDelay
			}
			jitterMax := backoff / 5
			if jitterMax == 0 {
				jitterMax = 1
			}
			jitter := time.Duration(p.randInt63n(int64(jitterMax))) * time.Nanosecond
			if p.randIntn(2) == 0 {
				backoff = backoff + jitter
			} else {
				backoff = backoff - jitter
			}

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
			continue
		}
		channelID := uuid.New()
		bidi := muxer.RegisterWithContext(ctx, storeID, channelID)

		// we use a polymorphic envelope so that we can
		// unmarshal server side and route the msg
		envelope := polymorphic.NewEnvelope(msg)
		if err := bidi.Encode(envelope); err != nil {
			bidi.Close(err)
			lastErr = err
			// If muxer was closed, attempt reconnect and retry
			if errors.Is(err, ErrMuxerClosed) || err == ErrMuxerClosed {
				slog.WarnContext(ctx, "provider: muxer closed during encode, retrying",
					slog.String("store_id", storeID.String()),
					slog.Int("attempt", attempt),
					slog.Int("max_attempts", maxEncodeAttempts),
					slog.String("channel_id", channelID.String()),
					slog.String("error_type", classifyTransportError(err)))

				// Force invalidate the current muxer to ensure next attempt creates fresh connection
				p.invalidateMuxer()

				// exponential backoff with jitter: baseDelay * 2^(attempt-1) +/- up to 10% jitter
				backoff := baseDelay * (1 << uint(attempt-1))
				if backoff > maxDelay {
					backoff = maxDelay
				}
				// Add jitter: +/- 10% of backoff (ensure n >= 1 to avoid randInt63n(0) panic)
				jitterMax := backoff / 5
				if jitterMax == 0 {
					jitterMax = 1
				}
				jitter := time.Duration(p.randInt63n(int64(jitterMax))) * time.Nanosecond
				if p.randIntn(2) == 0 {
					backoff = backoff + jitter
				} else {
					backoff = backoff - jitter
				}

				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(backoff):
				}
				continue
			}
			// Non-muxer errors are fatal (context cancellation, auth errors, etc.)
			telemetry.RecordError(span, err)
			slog.WarnContext(ctx, "provider: encode failed with non-transient error",
				slog.String("store_id", storeID.String()),
				slog.String("channel_id", channelID.String()),
				slog.String("error_type", classifyTransportError(err)),
				slog.String("error", err.Error()),
			)
			return nil, err
		}
		return bidi, nil
	}

	if lastErr != nil {
		telemetry.RecordError(span, lastErr)
		slog.ErrorContext(ctx, "provider: exhausted all retry attempts",
			slog.String("store_id", storeID.String()),
			slog.Int("max_attempts", maxEncodeAttempts),
			slog.String("error_type", classifyTransportError(lastErr)),
			slog.String("lastError", lastErr.Error()))
		return nil, lastErr
	}
	err := errors.New("failed to call stream")
	telemetry.RecordError(span, err)
	return nil, err
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
					select {
					case <-p.reconnectCtx.Done():
						return
					case <-time.After(500 * time.Millisecond):
					}
					continue
				}

				// attempt to dial and create a new muxer
				var conn *websocket.Conn
				var err error
				var permanentErr bool
				if p.dialFn != nil {
					conn, err = p.dialFn()
				} else {
					conn, err = p.dial()
				}
				success := false
				if err == nil {
					// Check if provider was stopped during dial
					if p.stopped.Load() {
						if conn != nil {
							conn.Close()
						}
						p.dialing.Store(false)
						return
					}

					// Guard: if another goroutine already installed a healthy muxer
					// while we were dialing, discard this connection to avoid
					// replacing a working muxer and killing in-flight streams.
					p.mu.Lock()
					alreadyHealthy := p.muxer != nil && p.muxer.Ping()
					p.mu.Unlock()
					if alreadyHealthy {
						if conn != nil {
							conn.Close()
						}
						p.dialing.Store(false)
						backoff = time.Second
						continue
					}

					// Issue #25: Use reconnectCtx instead of context.Background()
					// so muxer goroutines can be canceled during provider shutdown
					m2 := p.newClientMuxer(p.reconnectCtx, NewClientMuxerSession(), conn)
					if m2 != nil {
						isReconnect := p.hasConnected.Swap(true)
						p.replaceMuxer(m2)
						slog.Debug("provider: reconnect successful")
						success = true

						if isReconnect {
							go p.notifyReconnected(p.reconnectCtx, uuid.Nil)
						}
					}
				} else {
					if p.OnDialFailure != nil {
						p.OnDialFailure(err)
					}
					permanentErr = p.isPermanentDialError(err)
					slog.Warn("provider: reconnect dial failed",
						slog.String("error", err.Error()),
						slog.String("error_type", classifyTransportError(err)),
						slog.Bool("permanent", permanentErr))
					if permanentErr {
						// Don't hammer the server: use a single longer backoff before next attempt.
						backoff = 30 * time.Second
					}
				}

				// Release dialing lock before sleeping
				p.dialing.Store(false)

				// Backoff: reset on success, escalate on transient failure
				if success {
					backoff = time.Second
				} else if !permanentErr && backoff < 30*time.Second {
					backoff *= 2
				}

				// jittered backoff before next attempt
				select {
				case <-p.reconnectCtx.Done():
					return
				case <-time.After(backoff + time.Duration(p.randInt63n(500))*time.Millisecond):
				}
			}
		}()
	})
}

// getOrCreateMuxer dials and initializes or returns the current muxer.
// Implements coordinated reconnect: if a dial is in progress, waits for completion
// rather than failing immediately, enabling resilient concurrent recovery.
func (p *WebSocketBidiStreamProvider) getOrCreateMuxer(ctx context.Context) (providerMuxer, error) {
	// Fast path: existing healthy muxer.
	p.mu.Lock()
	if p.muxer != nil && p.muxer.Ping() {
		m := p.muxer
		p.mu.Unlock()
		return m, nil
	}
	p.mu.Unlock()

	// Ensure the background reconnect loop is running. It retries indefinitely
	// with backoff and is the single owner of dialing when no foreground caller
	// holds the lock. On the very first call this starts the goroutine; the
	// reconnectOnce guard makes subsequent calls a no-op.
	p.startReconnectLoop()

	// Try to become the dialing goroutine for a quick inline attempt.
	if p.dialing.CompareAndSwap(false, true) {
		conn, err := p.dialOnce()
		if err == nil {
			// Use reconnectCtx (not the caller's ctx) so the muxer outlives the
			// request that triggered its creation. A short-lived request context
			// would cancel the muxer's goroutines while it's still installed.
			m := p.newClientMuxer(p.reconnectCtx, NewClientMuxerSession(), conn)
			isReconnect := p.hasConnected.Swap(true)
			p.replaceMuxer(m)
			p.dialing.Store(false)

			if isReconnect {
				slog.DebugContext(ctx, "provider: muxer recreated after reconnection")
				go p.notifyReconnected(p.reconnectCtx, uuid.Nil)
			} else {
				slog.DebugContext(ctx, "provider: initial muxer created")
			}
			return m, nil
		}
		p.dialing.Store(false)
		// Inline attempt failed; fall through to wait on the background loop.
	}

	// Wait for the background reconnect loop to establish a healthy muxer.
	// This avoids burning a full 5-attempt foreground dial cycle per request
	// while the background loop is already retrying.
	pollInterval := 250 * time.Millisecond
	timer := time.NewTimer(time.Duration(p.maxDialAttempts*6) * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, errors.New("timed out waiting for stream connection")
		case <-time.After(pollInterval):
			p.mu.Lock()
			if p.muxer != nil && p.muxer.Ping() {
				m := p.muxer
				p.mu.Unlock()
				return m, nil
			}
			p.mu.Unlock()
		}
	}
}

// dialOnce performs a single dial attempt. Extracted so getOrCreateMuxer can
// make one quick inline try before falling back to the background loop.
func (p *WebSocketBidiStreamProvider) dialOnce() (*websocket.Conn, error) {
	if p.dialFn != nil {
		return p.dialFn()
	}
	return p.dial()
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
// The old muxer is closed after a grace period to allow in-flight streams
// on the old muxer to complete — matching replaceMuxer's behavior.
// Returns true if there was an existing muxer that will be closed.
func (p *WebSocketBidiStreamProvider) invalidateMuxer() bool {
	p.mu.Lock()
	oldMuxer := p.muxer
	p.muxer = nil
	p.mu.Unlock()

	// Close old muxer outside the lock with a grace period to allow
	// in-flight operations to complete. Without this delay, concurrent
	// goroutines that already obtained a reference to this muxer via
	// getOrCreateMuxer and are mid-Register/Encode get killed, causing
	// cascading ErrMuxerClosed failures.
	if oldMuxer != nil {
		go func(m providerMuxer) {
			// Wait for any in-flight encode operations to complete
			time.Sleep(500 * time.Millisecond)
			p.closeMuxer(m)
		}(oldMuxer)
		return true
	}
	return false
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
	}

	return nil
}

// isPermanentDialError returns true for auth-related errors (401, unauthorized,
// etc.) so the provider can fail fast instead of burning retries, unless
// RetryAuthFailures is enabled.
//
// When RetryAuthFailures is true, auth failures are treated as transient — this is
// useful during JWKS propagation delays where a valid token may be temporarily rejected
// until the new signing key is fully propagated.
//
// Server-side: When 401s persist in logs, check signing key/algorithm, audience/issuer
// mismatch, token expiry or clock skew, and that the path does not return 401 for
// non-auth reasons. Inspect server logs and JWT validation config.
func (p *WebSocketBidiStreamProvider) isPermanentDialError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	permanent := []string{"401", "unauthorized", "authentication failed", "forbidden", "permission denied"}
	for _, sub := range permanent {
		if strings.Contains(s, sub) {
			// If RetryAuthFailures is enabled, treat auth errors as transient
			if p.RetryAuthFailures {
				return false
			}
			return true
		}
	}
	return false
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
			slog.WarnContext(ctx, "reconnect listener failed",
				slog.String("listener_type", fmt.Sprintf("%T", listener)),
				slog.String("store_id", storeID.String()),
				slog.String("error_type", classifyTransportError(err)),
				"err", err)
		}
	}
}
