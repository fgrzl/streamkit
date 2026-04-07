package wskit

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

// controlMsgSentinel is a prefix that all muxer-level control messages must carry
// in their Type field. Application domain objects may validly have a "type" JSON
// field; without this prefix they would previously be silently consumed as
// control messages and never delivered to the application.
const controlMsgSentinel = "mux::"

// MuxerBidiStream is a bidirectional stream abstraction for use with a WebSocketMuxer.
// It is envelope-agnostic and operates on raw JSON payloads.
type MuxerBidiStream struct {
	encode     func([]byte) error
	recvChan   chan any
	closeOnce  sync.Once
	closed     chan struct{}
	closedFlag uint32 // 0 == open, 1 == closed
	onClose    func()
	channelID  uuid.UUID
}

// NewMuxerBidiStream creates a new MuxerBidiStream.
// `encode` is a function to send outbound messages as JSON.
// `onClose` is an optional cleanup callback invoked once upon stream close.
func NewMuxerBidiStream(
	encode func([]byte) error,
	onClose func(),
) *MuxerBidiStream {
	s := &MuxerBidiStream{
		encode:   encode,
		recvChan: make(chan any, 256),
		closed:   make(chan struct{}),
		onClose:  onClose,
	}
	return s
}

// Encode marshals the given message and sends it via the provided encode function.
func (c *MuxerBidiStream) Encode(m any) error {
	payload, err := json.Marshal(m)
	if err != nil {
		slog.Error("bidi: encode marshal failed",
			slog.String("channel_id", c.channelID.String()),
			slog.String("error_type", "marshal"),
			"err", err)
		return err
	}
	if err := c.encode(payload); err != nil {
		// Send failures are almost always disconnect/teardown; callers get err for handling.
		// Avoid Error/Warn here — it dominated logs during normal reconnect flows.
		slog.Debug("bidi: encode send failed",
			slog.String("channel_id", c.channelID.String()),
			slog.String("error_type", classifyTransportError(err)),
			slog.Int("bytes", len(payload)),
			"err", err)
		return err
	}
	return nil
}

// Decode blocks until a message is received or the stream is closed.
func (c *MuxerBidiStream) Decode(v any) error {
	select {
	case <-c.closed:
		return io.EOF

	case msg, ok := <-c.recvChan:
		if !ok {
			return io.EOF
		}
		payload, err := c.payloadFromMsg(msg)
		if err != nil {
			return err
		}

		// Check whether payload represents a control ErrorMessage and handle it
		if handled, err := c.handleErrorMessage(payload); handled {
			return err
		}

		// Normal decode
		if err := json.Unmarshal(payload, v); err != nil {
			slog.Warn("bidi: decode unmarshal failed",
				slog.String("channel_id", c.channelID.String()),
				slog.String("error_type", "decode"),
				slog.Int("bytes", len(payload)),
				"err", err)
			return err
		}
		return nil
	}
}

// payloadFromMsg converts the incoming message into a JSON payload byte slice.
func (c *MuxerBidiStream) payloadFromMsg(msg any) ([]byte, error) {
	switch m := msg.(type) {
	case []byte:
		return m, nil
	case string:
		return []byte(m), nil
	default:
		b, err := json.Marshal(m)
		if err != nil {
			return nil, err
		}
		return b, nil
	}
}

// handleErrorMessage inspects the payload for an ErrorMessage. If the payload
// contains a muxer control message (type prefixed with controlMsgSentinel), it
// handles logging and returns (true, err) where err is the appropriate error to
// return from Decode (or nil for EOF). If the payload is not a control message,
// (false, nil) is returned. Domain objects that happen to have a "type" JSON
// field are not affected since they lack the controlMsgSentinel prefix.
func (c *MuxerBidiStream) handleErrorMessage(payload []byte) (bool, error) {
	var errMsg ErrorMessage
	if err := json.Unmarshal(payload, &errMsg); err != nil || !strings.HasPrefix(errMsg.Type, controlMsgSentinel) {
		return false, nil
	}

	switch errMsg.Type {
	case controlMsgSentinel + "close":
		if errMsg.Err != "" {
			remoteErr := errors.New(errMsg.Err)
			if benignDisconnect(remoteErr) {
				slog.Debug("bidi: remote closed stream",
					slog.String("channel_id", c.channelID.String()),
					slog.String("error_type", classifyTransportError(remoteErr)),
					slog.String("detail", errMsg.Err))
			} else {
				slog.Warn("bidi: remote closed stream with error",
					slog.String("channel_id", c.channelID.String()),
					slog.String("error_type", classifyTransportError(remoteErr)),
					slog.String("err", errMsg.Err))
			}
			return true, fmt.Errorf("remote closed stream: %s", errMsg.Err)
		}
		return true, io.EOF
	case controlMsgSentinel + "error":
		remoteErr := errors.New(errMsg.Err)
		slog.Warn("bidi: remote error",
			slog.String("channel_id", c.channelID.String()),
			slog.String("error_type", classifyTransportError(remoteErr)),
			slog.String("err", errMsg.Err))
		return true, fmt.Errorf("remote error: %s", errMsg.Err)
	default:
		slog.Warn("bidi: unknown error type",
			slog.String("channel_id", c.channelID.String()),
			slog.String("error_type", "protocol"),
			slog.String("type", errMsg.Type),
			slog.String("err", errMsg.Err))
		return true, fmt.Errorf("unknown error type %q: %s", errMsg.Type, errMsg.Err)
	}
}

// CloseSend sends a JSON close message to the remote side.
func (c *MuxerBidiStream) CloseSend(err error) error {
	msg := &ErrorMessage{
		Type: controlMsgSentinel + "close",
	}
	if err != nil {
		msg.Err = err.Error()
	}
	if encErr := c.Encode(msg); encErr != nil {
		return encErr
	}
	return nil
}

// Close tears down the stream and invokes the onClose hook.
func (c *MuxerBidiStream) Close(err error) {
	c.closeOnce.Do(func() {
		// Attempt to notify remote of close; on network errors this may fail.
		_ = c.CloseSend(err)
		// mark closed and notify listeners; do not close recvChan to avoid send-on-closed panics
		atomic.StoreUint32(&c.closedFlag, 1)
		close(c.closed)

		// Issue #21: Drain buffered messages to prevent memory leak.
		// Messages buffered in recvChan would leak until GC collects this struct.
		for {
			select {
			case <-c.recvChan:
				// Continue draining
			default:
				// Channel is empty, we can stop
				goto drainDone
			}
		}
	drainDone:

		if c.onClose != nil {
			c.onClose()
		}
	})
}

// CloseLocal marks the stream as closed and runs the onClose hook, but
// it does not attempt to send a close message to the remote side. Use
// this when the network is unavailable and you still need to tear down
// local resources without triggering additional network I/O.
func (c *MuxerBidiStream) CloseLocal(err error) {
	c.closeOnce.Do(func() {
		// Do not call CloseSend since network may be down.
		atomic.StoreUint32(&c.closedFlag, 1)
		close(c.closed)

		// Drain buffered messages to prevent memory leak (mirrors Close drain).
		for {
			select {
			case <-c.recvChan:
			default:
				goto drainDone
			}
		}
	drainDone:

		if c.onClose != nil {
			c.onClose()
		}
	})
}

// RecvChan returns the channel for incoming messages.
func (c *MuxerBidiStream) RecvChan() chan<- any {
	return c.recvChan
}

// Offer attempts to deliver a message to the stream's receive channel.
// It returns true if the message was delivered, or false if the stream
// is closed or the delivery failed. This method recovers from a possible
// panic caused by sending on a closed channel to be defensive against
// races between senders and Close().
func (c *MuxerBidiStream) Offer(msg any) (ok bool) {
	// Fast-path: if closed, skip without attempting to send.
	if atomic.LoadUint32(&c.closedFlag) != 0 {
		return false
	}

	select {
	case c.recvChan <- msg:
		return true
	case <-c.closed:
		return false
	}
}

// IsClosed reports whether the stream has been closed.
func (c *MuxerBidiStream) IsClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

func (c *MuxerBidiStream) Closed() <-chan struct{} {
	return c.closed
}

// EndOfStreamError returns the canonical EOF sentinel.
func (c *MuxerBidiStream) EndOfStreamError() error {
	return io.EOF
}

type ErrorMessage struct {
	Type string `json:"type"`
	Err  string `json:"err,omitempty"`
}

// SetChannelID attaches a channel ID to the stream for contextual logging.
func (c *MuxerBidiStream) SetChannelID(id uuid.UUID) {
	c.channelID = id
}
