package wskit

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	closeErrMu sync.Mutex
	closeErr   error
	onClose    func()
	channelID  uuid.UUID
	routeMu    sync.RWMutex
	routeOp    string
	routeSpace string
	routeSeg   string
}

// NewMuxerBidiStream creates a new MuxerBidiStream.
// `encode` is a function to send outbound messages as JSON.
// `onClose` is an optional cleanup callback invoked once upon stream close.
func NewMuxerBidiStream(
	encode func([]byte) error,
	onClose func(),
	queueSize ...int,
) *MuxerBidiStream {
	capacity := defaultStreamRecvQueueSize
	if len(queueSize) > 0 && queueSize[0] > 0 {
		capacity = queueSize[0]
	}
	s := &MuxerBidiStream{
		encode:   encode,
		recvChan: make(chan any, capacity),
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
	msg, ok, err := c.recv()
	if err != nil {
		return err
	}
	if !ok {
		return c.closedErr()
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

func (c *MuxerBidiStream) recv() (any, bool, error) {
	select {
	case msg := <-c.recvChan:
		return msg, true, nil
	default:
	}

	select {
	case msg := <-c.recvChan:
		return msg, true, nil
	case <-c.closed:
		return nil, false, c.closedErr()
	}
}

func (c *MuxerBidiStream) closedErr() error {
	c.closeErrMu.Lock()
	defer c.closeErrMu.Unlock()
	if c.closeErr != nil {
		return c.closeErr
	}
	return io.EOF
}

func (c *MuxerBidiStream) recordCloseErr(err error) {
	c.closeErrMu.Lock()
	defer c.closeErrMu.Unlock()
	if c.closeErr == nil || err != nil {
		c.closeErr = err
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
			remoteErr := fmt.Errorf("remote closed stream: %s", errMsg.Err)
			c.CloseRemote(remoteErr)
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
			return true, remoteErr
		}
		c.CloseRemote(nil)
		return true, io.EOF
	case controlMsgSentinel + "error":
		remoteErr := fmt.Errorf("remote error: %s", errMsg.Err)
		c.CloseRemote(remoteErr)
		slog.Warn("bidi: remote error",
			slog.String("channel_id", c.channelID.String()),
			slog.String("error_type", classifyTransportError(remoteErr)),
			slog.String("err", errMsg.Err))
		return true, remoteErr
	default:
		protocolErr := fmt.Errorf("unknown error type %q: %s", errMsg.Type, errMsg.Err)
		c.CloseRemote(protocolErr)
		slog.Warn("bidi: unknown error type",
			slog.String("channel_id", c.channelID.String()),
			slog.String("error_type", "protocol"),
			slog.String("type", errMsg.Type),
			slog.String("err", errMsg.Err))
		return true, protocolErr
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
	c.close(true, true, err)
}

// CloseLocal marks the stream as closed and runs the onClose hook, but
// it does not attempt to send a close message to the remote side. Use
// this when the network is unavailable and you still need to tear down
// local resources without triggering additional network I/O.
func (c *MuxerBidiStream) CloseLocal(err error) {
	c.close(false, true, err)
}

// CloseRemote marks the stream as closed because the peer ended the logical
// stream. Buffered messages are preserved so callers can finish draining any
// payloads that arrived before the terminal signal.
func (c *MuxerBidiStream) CloseRemote(err error) {
	c.close(false, false, err)
}

func (c *MuxerBidiStream) close(notifyRemote, drainBuffered bool, err error) {
	c.closeOnce.Do(func() {
		if notifyRemote {
			// Attempt to notify remote of close; on network errors this may fail.
			_ = c.CloseSend(err)
		}
		c.recordCloseErr(err)
		atomic.StoreUint32(&c.closedFlag, 1)
		close(c.closed)

		if drainBuffered {
			for {
				select {
				case <-c.recvChan:
				default:
					goto drainDone
				}
			}
		drainDone:
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
		return false
	}

	select {
	case c.recvChan <- msg:
		return true
	case <-c.closed:
		return false
	default:
		return false
	}
}

// OfferWithin attempts to deliver a message to the stream, waiting up to the
// provided timeout for receive buffer headroom before giving up.
func (c *MuxerBidiStream) OfferWithin(msg any, timeout time.Duration) bool {
	if timeout <= 0 {
		return c.Offer(msg)
	}
	if atomic.LoadUint32(&c.closedFlag) != 0 {
		return false
	}

	select {
	case c.recvChan <- msg:
		return true
	case <-c.closed:
		return false
	default:
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case c.recvChan <- msg:
		return true
	case <-c.closed:
		return false
	case <-timer.C:
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

// SetRouteDebugDetails attaches stream route identity used by transport logs.
func (c *MuxerBidiStream) SetRouteDebugDetails(operation, space, segment string) {
	c.routeMu.Lock()
	defer c.routeMu.Unlock()
	c.routeOp = operation
	c.routeSpace = space
	c.routeSeg = segment
}

func (c *MuxerBidiStream) routeDebugDetails() (operation, space, segment string) {
	c.routeMu.RLock()
	defer c.routeMu.RUnlock()
	return c.routeOp, c.routeSpace, c.routeSeg
}
