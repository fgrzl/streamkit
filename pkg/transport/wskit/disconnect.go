package wskit

import (
	"errors"
	"io"
	"net"
	"strings"
	"syscall"
)

// benignDisconnect reports whether err typically indicates normal or expected
// transport teardown (peer close, RST, pipe closed, local muxer shutdown)
// rather than an actionable application error.
func benignDisconnect(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	if errors.Is(err, ErrMuxerClosed) {
		return true
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr != nil {
		return benignDisconnect(opErr.Err)
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "forcibly closed by the remote host") ||
		strings.Contains(msg, "connection aborted") ||
		strings.Contains(msg, "use of closed network connection")
}
