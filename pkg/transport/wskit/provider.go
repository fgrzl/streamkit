package wskit

import (
	"context"
	"net/http"
	"sync"

	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
	"golang.org/x/net/websocket"
)

// WebSocketBidiStreamProvider manages a per-tenant WebSocket connection and muxer.
type WebSocketBidiStreamProvider struct {
	addr   string
	origin string
	token  string

	mu    sync.Mutex
	muxer *WebSocketMuxer
}

// NewBidiStreamProvider creates a provider that uses a dedicated WebSocket connection per tenant.
func NewBidiStreamProvider(addr, token string) api.BidiStreamProvider {
	return &WebSocketBidiStreamProvider{
		addr:   addr,
		origin: "http://localhost",
		token:  token,
	}
}

// CallStream opens or reuses a muxed stream over the WebSocket for a single logical interaction.
func (p *WebSocketBidiStreamProvider) CallStream(ctx context.Context, msg api.Routeable) (api.BidiStream, error) {
	muxer, err := p.getOrCreateMuxer()
	if err != nil {
		return nil, err
	}

	stream := muxer.Register(uuid.New())

	if err := stream.Encode(msg); err != nil {
		stream.Close(err)
		return nil, err
	}

	return stream, nil
}

// getOrCreateMuxer dials and initializes the WebSocket muxer if needed.
func (p *WebSocketBidiStreamProvider) getOrCreateMuxer() (*WebSocketMuxer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.muxer != nil {
		return p.muxer, nil
	}

	conn, err := p.dial()
	if err != nil {
		return nil, err
	}

	muxer := NewWebSocketMuxer(conn)
	p.muxer = muxer
	return muxer, nil
}

// dial establishes the raw WebSocket connection with token-based auth.
func (p *WebSocketBidiStreamProvider) dial() (*websocket.Conn, error) {
	cfg, err := websocket.NewConfig(p.addr, p.origin)
	if err != nil {
		return nil, err
	}

	cfg.Header = http.Header{}
	cfg.Header.Set("Authorization", "Bearer "+p.token)

	return websocket.DialConfig(cfg)
}
