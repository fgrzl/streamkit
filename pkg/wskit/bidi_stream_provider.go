package wskit

import (
	"context"
	"sync"

	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
)

type WebSocketBidiStreamProvider struct {
	dialer      WebSocketDialer
	clientMux   *WebSocketMuxer
	muxesMu     sync.Mutex
	handlersMu  sync.RWMutex
	defaultAddr string
}

// NewWebSocketBus creates a StreamBus using a default connection target.
func NewWebSocketBus(dialer WebSocketDialer, defaultAddress string) (api.BidiStreamProvider, error) {
	return &WebSocketBidiStreamProvider{
		dialer:      dialer,
		defaultAddr: defaultAddress,
	}, nil
}

// CallStream sends an initial message and returns a BidiStream for continued use.
func (b *WebSocketBidiStreamProvider) CallStream(ctx context.Context, msg api.Routeable) (api.BidiStream, error) {
	muxer := b.getOrCreateClientMuxer(ctx)
	stream := muxer.Register(uuid.New())

	if err := stream.Encode(msg); err != nil {
		stream.Close(err)
		return nil, err
	}

	return stream, nil
}

func (b *WebSocketBidiStreamProvider) getOrCreateClientMuxer(ctx context.Context) *WebSocketMuxer {
	b.muxesMu.Lock()
	defer b.muxesMu.Unlock()

	if b.clientMux != nil {
		return b.clientMux
	}

	conn, err := b.dialer.Dial(ctx, b.defaultAddr)
	if err != nil {
		panic("dial failed: " + err.Error())
	}

	mux := NewWebSocketMuxer(conn)
	b.clientMux = mux
	return mux
}
