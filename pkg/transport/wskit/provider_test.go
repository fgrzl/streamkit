package wskit

import (
	"context"
	"errors"
	"testing"
	"time"

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
