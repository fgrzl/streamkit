package wskit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/websocket"
)

func TestNewBidiStreamProviderAddressNormalization(t *testing.T) {
	p := NewBidiStreamProvider("https://example.com/", func() (string, error) { return "tok", nil })
	wp, ok := p.(*WebSocketBidiStreamProvider)
	if !ok {
		t.Fatalf("expected provider type")
	}
	// ensure address ends with /streamz and no double slashes
	assert.Equal(t, "https://example.com/streamz", wp.addr)
}

func TestGetOrCreateMuxerRetries(t *testing.T) {
	p := NewBidiStreamProvider("https://example.com/", func() (string, error) { return "tok", nil }).(*WebSocketBidiStreamProvider)

	// simulate dialFn always failing to ensure backoff and attempts are exercised
	calls := 0
	p.maxDialAttempts = 3
	p.dialFn = func() (*websocket.Conn, error) {
		calls++
		return nil, errors.New("dial failed")
	}

	// call getOrCreateMuxer and ensure it returns an error after exhausting attempts
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m, err := p.getOrCreateMuxer(ctx)
	// the fake dialFn returns nil conn, so NewClientWebSocketMuxer will panic or misbehave;
	// instead we expect an error or nil muxer; the important assertion here is that dialFn was called multiple times
	assert.Equal(t, 3, calls, "expected dialFn to be called until max attempts")
	// ensure returned muxer is nil (since dialFn didn't produce a usable connection)
	assert.Nil(t, m)
	assert.NotNil(t, err)
}
