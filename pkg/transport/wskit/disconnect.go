package wskit

import (
	"context"
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

// classifyTransportError returns a stable class for transport-related failures so
// logs can group common failure modes without parsing free-form error strings.
func classifyTransportError(err error) string {
	if err == nil {
		return "none"
	}
	if errors.Is(err, context.Canceled) {
		return "context_canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if errors.Is(err, ErrMuxerClosed) {
		return "muxer_closed"
	}
	if errors.Is(err, ErrHeartbeatTimeout) {
		return "heartbeat_timeout"
	}
	if benignDisconnect(err) {
		return "disconnect"
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "network_timeout"
		}
		return "network"
	}

	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "401") ||
		strings.Contains(msg, "unauthorized") ||
		strings.Contains(msg, "authentication failed") ||
		strings.Contains(msg, "forbidden") ||
		strings.Contains(msg, "permission denied") {
		return "auth"
	}
	if strings.Contains(msg, "timeout") {
		return "timeout"
	}

	return "other"
}
