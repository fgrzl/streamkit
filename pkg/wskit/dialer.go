package wskit

import (
	"context"
	"net/http"

	"golang.org/x/net/websocket"
)

// WebSocketDialer defines how to establish outbound websocket connections.
type WebSocketDialer interface {
	Dial(ctx context.Context, address string) (*websocket.Conn, error)
}

type DefaultWebSocketDialer struct {
	TokenFunc func(ctx context.Context) (string, error)
}

// DialWebSocket implements WebSocketDialer.
func (d *DefaultWebSocketDialer) Dial(ctx context.Context, address string) (*websocket.Conn, error) {
	origin := "http://localhost/" // required but not validated in most WS servers

	token, err := d.TokenFunc(ctx)
	if err != nil {
		return nil, err
	}

	cfg, err := websocket.NewConfig(address, origin)
	if err != nil {
		return nil, err
	}

	cfg.Header = http.Header{}
	cfg.Header.Set("Authorization", "Bearer "+token)

	return websocket.DialConfig(cfg)
}

// NewDefaultWebSocketDialer creates a default dialer that pulls a bearer token from context.
func NewDefaultWebSocketDialer(tokenFunc func(ctx context.Context) (string, error)) WebSocketDialer {
	return &DefaultWebSocketDialer{
		TokenFunc: tokenFunc,
	}
}
