package wskit

import (
	"context"
	"net/http"
	"sync"

	"github.com/fgrzl/json/polymorphic"
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

	if p.muxer != nil {
		return p.muxer, nil
	}

	conn, err := p.dial()
	if err != nil {
		return nil, err
	}

	muxer := NewClientWebSocketMuxer(ctx, NewClientMuxerSession(), conn)
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
