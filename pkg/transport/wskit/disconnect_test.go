package wskit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBenignDisconnect(t *testing.T) {
	t.Parallel()
	assert.True(t, benignDisconnect(io.EOF))
	assert.True(t, benignDisconnect(fmt.Errorf("wrap: %w", io.EOF)))
	assert.True(t, benignDisconnect(ErrMuxerClosed))
	assert.True(t, benignDisconnect(net.ErrClosed))
	assert.True(t, benignDisconnect(syscall.ECONNRESET))
	assert.True(t, benignDisconnect(syscall.EPIPE))
	assert.True(t, benignDisconnect(&net.OpError{Err: syscall.ECONNRESET}))
	assert.True(t, benignDisconnect(errors.New("connection reset by peer")))
	assert.True(t, benignDisconnect(errors.New("broken pipe")))
	assert.False(t, benignDisconnect(nil))
	assert.False(t, benignDisconnect(errors.New("invalid payload")))
}

func TestClassifyTransportError(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "context_canceled", classifyTransportError(context.Canceled))
	assert.Equal(t, "deadline_exceeded", classifyTransportError(context.DeadlineExceeded))
	assert.Equal(t, "heartbeat_timeout", classifyTransportError(ErrHeartbeatTimeout))
	assert.Equal(t, "disconnect", classifyTransportError(io.EOF))
	assert.Equal(t, "auth", classifyTransportError(errors.New("401 unauthorized")))
	assert.Equal(t, "network", classifyTransportError(&net.OpError{Err: errors.New("dial tcp failed")}))
	assert.Equal(t, "other", classifyTransportError(errors.New("invalid payload")))
}
