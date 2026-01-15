package wskit

import (
	"context"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
	"golang.org/x/net/websocket"
)

// WebSocketBidiStreamProvider manages a per-tenant WebSocket connection and muxer.
type WebSocketBidiStreamProvider struct {
	addr     string
	origin   string
	fetchJWT func() (string, error)

	mu    sync.Mutex
	muxer *WebSocketMuxer
	// reconnect RNG for jittered backoff
	rng *rand.Rand
	// testable hooks / configuration
	dialFn          func() (*websocket.Conn, error)
	maxDialAttempts int
}

// NewBidiStreamProvider creates a provider that uses a dedicated WebSocket connection per client.
func NewBidiStreamProvider(addr string, fetchJWT func() (string, error)) api.BidiStreamProvider {

	// normalize address: ensure it ends with /streamz and avoid duplicate slashes
	// trim any trailing slashes, then append the segment
	a := strings.TrimRight(addr, "/")
	a = a + "/streamz"

	return &WebSocketBidiStreamProvider{
		addr:     a,
		origin:   "http://localhost",
		fetchJWT: fetchJWT,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
		// sensible defaults; tests may override
		dialFn:          nil,
		maxDialAttempts: 5,
	}
}

// CallStream opens or reuses a muxed stream over the WebSocket for a single logical interaction.
func (p *WebSocketBidiStreamProvider) CallStream(ctx context.Context, storeID uuid.UUID, msg api.Routeable) (api.BidiStream, error) {

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
		return nil, err
	}
	return bidi, nil
}

// getOrCreateMuxer dials and initializes the WebSocket muxer if needed.
func (p *WebSocketBidiStreamProvider) getOrCreateMuxer(ctx context.Context) (*WebSocketMuxer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// If existing muxer is healthy, reuse it
	if p.muxer != nil && p.muxer.Ping() {
		return p.muxer, nil
	}

	// reconnect/backoff strategy: exponential backoff with jitter
	// quick attempts up to a ceiling to avoid infinite loops
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
			muxer := NewClientWebSocketMuxer(ctx, NewClientMuxerSession(), conn)
			p.muxer = muxer
			return muxer, nil
		}

		lastErr = err

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

		// release lock while sleeping to avoid blocking other callers
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			p.mu.Lock()
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		p.mu.Lock()
	}

	return nil, lastErr
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
