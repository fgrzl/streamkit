package wskit

import (
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
