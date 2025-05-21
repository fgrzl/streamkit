package wskit

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

// MuxerBidiStream is a bidirectional stream abstraction for use with a WebSocketMuxer.
// It is envelope-agnostic and operates on raw JSON payloads.
type MuxerBidiStream struct {
	encode   func([]byte) error
	recvChan chan any

	closeOnce sync.Once
	closed    chan struct{}
	onClose   func()
}

// NewMuxerBidiStream creates a new MuxerBidiStream.
// `encode` is a function to send outbound messages as JSON.
// `onClose` is an optional cleanup callback invoked once upon stream close.
func NewMuxerBidiStream(
	encode func([]byte) error,
	onClose func(),
) *MuxerBidiStream {
	return &MuxerBidiStream{
		encode:   encode,
		recvChan: make(chan any, 256),
		closed:   make(chan struct{}),
		onClose:  onClose,
	}
}

// Encode marshals the given message and sends it via the provided encode function.
func (c *MuxerBidiStream) Encode(m any) error {
	payload, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return c.encode(payload)
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

		// Check if it's a CloseMessage
		var closeMsg CloseMessage
		if err := json.Unmarshal(payload, &closeMsg); err == nil && closeMsg.Type == "close" {
			if closeMsg.Err != "" {
				return fmt.Errorf("remote closed stream: %s", closeMsg.Err)
			}
			return io.EOF
		}

		// Normal decode
		return json.Unmarshal(payload, v)
	}
}

// CloseSend sends a JSON close message to the remote side.
func (c *MuxerBidiStream) CloseSend(err error) error {
	msg := &CloseMessage{
		Type: "close",
	}
	if err != nil {
		msg.Err = err.Error()
	}
	return c.Encode(msg)
}

// Close tears down the stream and invokes the onClose hook.
func (c *MuxerBidiStream) Close(err error) {
	c.closeOnce.Do(func() {
		_ = c.CloseSend(err)
		close(c.recvChan)
		close(c.closed)
		if c.onClose != nil {
			c.onClose()
		}
	})
}

// RecvChan returns the channel for incoming messages.
func (c *MuxerBidiStream) RecvChan() chan<- any {
	return c.recvChan
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

type CloseMessage struct {
	Type string `json:"type"` // always "close"
	Err  string `json:"err,omitempty"`
}
