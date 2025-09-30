package wskit

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

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
	slog.Debug("bidi: stream created")
	return s
}

// Encode marshals the given message and sends it via the provided encode function.
func (c *MuxerBidiStream) Encode(m any) error {
	payload, err := json.Marshal(m)
	if err != nil {
		slog.Error("bidi: encode marshal failed", "err", err, slog.String("channel_id", c.channelID.String()))
		return err
	}
	if err := c.encode(payload); err != nil {
		slog.Error("bidi: encode send failed", "err", err, slog.String("channel_id", c.channelID.String()), slog.Int("bytes", len(payload)))
		return err
	}
	slog.Debug("bidi: sent message", slog.String("channel_id", c.channelID.String()), slog.Int("bytes", len(payload)))
	return nil
}

// Decode blocks until a message is received or the stream is closed.
func (c *MuxerBidiStream) Decode(v any) error {
	select {
	case <-c.closed:
		slog.Debug("bidi: decode on closed stream -> EOF", slog.String("channel_id", c.channelID.String()))
		return io.EOF

	case msg, ok := <-c.recvChan:
		if !ok {
			slog.Debug("bidi: recv channel closed -> EOF", slog.String("channel_id", c.channelID.String()))
			return io.EOF
		}

		var payload []byte
		switch m := msg.(type) {
		case []byte:
			payload = m
		case string:
			payload = []byte(m)
		default:
			var err error
			payload, err = json.Marshal(m)
			if err != nil {
				return err
			}
		}

		// Check if it's an ErrorMessage
		var errMsg ErrorMessage
		if err := json.Unmarshal(payload, &errMsg); err == nil && errMsg.Type != "" {
			switch errMsg.Type {
			case "close":
				if errMsg.Err != "" {
					slog.Warn("bidi: remote closed stream with error", slog.String("channel_id", c.channelID.String()), slog.String("err", errMsg.Err))
					return fmt.Errorf("remote closed stream: %s", errMsg.Err)
				}
				slog.Debug("bidi: remote closed stream", slog.String("channel_id", c.channelID.String()))
				return io.EOF
			case "error":
				slog.Warn("bidi: remote error", slog.String("channel_id", c.channelID.String()), slog.String("err", errMsg.Err))
				return fmt.Errorf("remote error: %s", errMsg.Err)
			default:
				slog.Warn("bidi: unknown error type", slog.String("channel_id", c.channelID.String()), slog.String("type", errMsg.Type), slog.String("err", errMsg.Err))
				return fmt.Errorf("unknown error type %q: %s", errMsg.Type, errMsg.Err)
			}
		}

		// Normal decode
		if err := json.Unmarshal(payload, v); err != nil {
			slog.Warn("bidi: decode unmarshal failed", slog.String("channel_id", c.channelID.String()), "err", err)
			return err
		}
		slog.Debug("bidi: received message", slog.String("channel_id", c.channelID.String()), slog.Int("bytes", len(payload)))
		return nil
	}
}

// CloseSend sends a JSON close message to the remote side.
func (c *MuxerBidiStream) CloseSend(err error) error {
	msg := &ErrorMessage{
		Type: "close",
	}
	if err != nil {
		msg.Err = err.Error()
	}
	if encErr := c.Encode(msg); encErr != nil {
		slog.Warn("bidi: failed to send close", slog.String("channel_id", c.channelID.String()), "err", encErr)
		return encErr
	}
	slog.Debug("bidi: sent close", slog.String("channel_id", c.channelID.String()), slog.String("reason", msg.Err))
	return nil
}

// Close tears down the stream and invokes the onClose hook.
func (c *MuxerBidiStream) Close(err error) {
	c.closeOnce.Do(func() {
		_ = c.CloseSend(err)
		// mark closed and notify listeners; do not close recvChan to avoid send-on-closed panics
		atomic.StoreUint32(&c.closedFlag, 1)
		close(c.closed)
		if err != nil {
			slog.Debug("bidi: stream closed", slog.String("channel_id", c.channelID.String()), slog.String("reason", err.Error()))
		} else {
			slog.Debug("bidi: stream closed", slog.String("channel_id", c.channelID.String()))
		}
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
		slog.Debug("bidi: offer on closed stream", slog.String("channel_id", c.channelID.String()))
		return false
	}

	select {
	case c.recvChan <- msg:
		slog.Debug("bidi: offered message", slog.String("channel_id", c.channelID.String()))
		return true
	case <-c.closed:
		slog.Debug("bidi: offer dropped, stream closed", slog.String("channel_id", c.channelID.String()))
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
	if id != uuid.Nil {
		slog.Debug("bidi: set channel id", slog.String("channel_id", id.String()))
	}
}
