package inprockit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streamkit/pkg/api"
)

type InProcBidiStream struct {
	sendChan    chan any
	recvChan    chan any
	recvOwned   bool
	sendClosed  sync.Once
	sendCloseMu sync.Mutex
	closeOnce   sync.Once
	closeErr    error
	closed      chan struct{}
}

var payloadBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

func NewInProcBidiStream() *InProcBidiStream {
	s := &InProcBidiStream{
		sendChan:  make(chan any, 64),
		recvChan:  make(chan any, 64),
		recvOwned: true,
		closed:    make(chan struct{}),
	}
	return s
}

// NewInProcBidiStreamLoopback returns a stream where Encode writes are
// delivered back to Decode on the same stream. This is useful for tests and
// local in-process usage where a single stream should loopback messages.
func NewInProcBidiStreamLoopback() *InProcBidiStream {
	s := NewInProcBidiStream()
	// Loopback: use the same channel for send and recv to avoid a forwarder goroutine.
	s.recvChan = s.sendChan
	s.recvOwned = false
	return s
}

func (s *InProcBidiStream) Encode(m any) error {
	if s.closeErr != nil {
		return s.closeErr
	}
	// For in-process streams we can avoid serialization overhead by sending
	// the object directly across the channel. The receiver will attempt a
	// direct type assignment before falling back to JSON decoding when needed.
	select {
	case <-s.closed:
		return io.ErrClosedPipe
	case s.sendChan <- m:
		return nil
	}
}

func (s *InProcBidiStream) Decode(v any) error {
	msg, ok := <-s.recvChan
	if !ok {
		return s.EndOfStreamError()
	}
	// Extract raw payloads (string/[]byte/*bytes.Buffer) and allow short-circuiting
	// for string destinations.
	payload, bufPtr, handled := extractRawPayload(msg, v)
	if handled {
		return nil
	}

	// Handle ErrorMessage control payloads.
	if handleErr, done := handleErrorMessage(msg, payload, bufPtr); done {
		return handleErr
	}

	// Try typed fast-paths for common hot types (envelope and api types).
	if typedHandled := typedFastPathAssign(msg, v); typedHandled == nil {
		return nil
	}

	// Fallback to reflect-based assignment if the message is a direct object.
	if reflectFallbackAssign(msg, v) == nil {
		return nil
	}

	// Finally, unmarshal JSON payload into v.
	if payload == nil {
		return fmt.Errorf("invalid message type %T", msg)
	}

	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return fmt.Errorf("Decode: expected non-nil pointer, got %T", v)
	}

	err := json.Unmarshal(payload, v)
	if bufPtr != nil {
		payloadBufPool.Put(bufPtr)
	}
	return err
}

// extractRawPayload handles raw payload types (string, []byte, *bytes.Buffer)
// and short-circuits assignment for *string destinations. It returns:
//   - payload []byte (may be nil),
//   - bufPtr *bytes.Buffer if we received a buffer (so caller can return it),
//   - handled bool indicating whether assignment was completed (and caller
//     should return nil).
func extractRawPayload(msg any, v any) (payload []byte, bufPtr *bytes.Buffer, handled bool) {
	switch m := msg.(type) {
	case []byte:
		if sv, ok := v.(*string); ok {
			*sv = string(m)
			return nil, nil, true
		}
		return m, nil, false
	case string:
		if sv, ok := v.(*string); ok {
			*sv = m
			return nil, nil, true
		}
		return []byte(m), nil, false
	case *bytes.Buffer:
		if sv, ok := v.(*string); ok {
			*sv = m.String()
			payloadBufPool.Put(m)
			return nil, nil, true
		}
		return m.Bytes(), m, false
	}
	return nil, nil, false
}

// handleErrorMessage attempts a cheap JSON scan for error control messages
// when we were given a byte payload. Returns (error, done) where done
// indicates caller should return (with provided error, which may be nil for
// EndOfStreamError).
func handleErrorMessage(msg any, payload []byte, bufPtr *bytes.Buffer) (error, bool) {
	if len(payload) > 0 && payload[0] == '{' && bytes.Contains(payload, []byte(`"type"`)) {
		var errMsg ErrorMessage
		if err := json.Unmarshal(payload, &errMsg); err == nil && errMsg.Type != "" {
			if bufPtr != nil {
				payloadBufPool.Put(bufPtr)
			}
			switch errMsg.Type {
			case "close":
				if errMsg.Err != "" {
					return fmt.Errorf("remote closed stream: %s", errMsg.Err), true
				}
				return io.EOF, true
			case "error":
				return fmt.Errorf("remote error: %s", errMsg.Err), true
			default:
				return fmt.Errorf("unknown error type %q: %s", errMsg.Type, errMsg.Err), true
			}
		}
	}
	// Also support direct ErrorMessage objects that were sent without JSON.
	switch m := msg.(type) {
	case ErrorMessage:
		if m.Type != "" {
			switch m.Type {
			case "close":
				if m.Err != "" {
					return fmt.Errorf("remote closed stream: %s", m.Err), true
				}
				return io.EOF, true
			case "error":
				return fmt.Errorf("remote error: %s", m.Err), true
			default:
				return fmt.Errorf("unknown error type %q: %s", m.Type, m.Err), true
			}
		}
	case *ErrorMessage:
		if m != nil && m.Type != "" {
			if m.Type == "close" {
				if m.Err != "" {
					return fmt.Errorf("remote closed stream: %s", m.Err), true
				}
				return io.EOF, true
			}
			if m.Type == "error" {
				return fmt.Errorf("remote error: %s", m.Err), true
			}
			return fmt.Errorf("unknown error type %q: %s", m.Type, m.Err), true
		}
	}
	return nil, false
}

// typedFastPathAssign tries to perform direct assignments for the hottest
// concrete types (polymorphic.Envelope and several api types). It returns
// nil on success (assignment done), or a non-nil sentinel (error) to mean
// "not handled".
func typedFastPathAssign(msg any, v any) error {
	switch mm := msg.(type) {
	case *polymorphic.Envelope:
		if ev, ok := v.(*polymorphic.Envelope); ok {
			*ev = *mm
			return nil
		}
		switch c := mm.Content.(type) {
		case *api.Produce:
			if pv, ok := v.(*api.Produce); ok {
				*pv = *c
				return nil
			}
		case api.Produce:
			if pv, ok := v.(*api.Produce); ok {
				*pv = c
				return nil
			}
		case *api.Consume:
			if pv, ok := v.(*api.Consume); ok {
				*pv = *c
				return nil
			}
		case api.Consume:
			if pv, ok := v.(*api.Consume); ok {
				*pv = c
				return nil
			}
		case *api.GetSpaces:
			if pv, ok := v.(*api.GetSpaces); ok {
				*pv = *c
				return nil
			}
		case api.GetSpaces:
			if pv, ok := v.(*api.GetSpaces); ok {
				*pv = c
				return nil
			}
		case *api.Peek:
			if pv, ok := v.(*api.Peek); ok {
				*pv = *c
				return nil
			}
		case api.Peek:
			if pv, ok := v.(*api.Peek); ok {
				*pv = c
				return nil
			}
		case *api.ConsumeSegment:
			if pv, ok := v.(*api.ConsumeSegment); ok {
				*pv = *c
				return nil
			}
		case api.ConsumeSegment:
			if pv, ok := v.(*api.ConsumeSegment); ok {
				*pv = c
				return nil
			}
		case *api.SubscribeToSegmentStatus:
			if pv, ok := v.(*api.SubscribeToSegmentStatus); ok {
				*pv = *c
				return nil
			}
		case api.SubscribeToSegmentStatus:
			if pv, ok := v.(*api.SubscribeToSegmentStatus); ok {
				*pv = c
				return nil
			}
		case *api.SegmentNotification:
			if pv, ok := v.(*api.SegmentNotification); ok {
				*pv = *c
				return nil
			}
		case api.SegmentNotification:
			if pv, ok := v.(*api.SegmentNotification); ok {
				*pv = c
				return nil
			}
		}

	case *api.Produce:
		if pv, ok := v.(*api.Produce); ok {
			*pv = *mm
			return nil
		}
	case api.Produce:
		if pv, ok := v.(*api.Produce); ok {
			*pv = mm
			return nil
		}
	case *api.Consume:
		if pv, ok := v.(*api.Consume); ok {
			*pv = *mm
			return nil
		}
	case api.Consume:
		if pv, ok := v.(*api.Consume); ok {
			*pv = mm
			return nil
		}
	case *api.GetSpaces:
		if pv, ok := v.(*api.GetSpaces); ok {
			*pv = *mm
			return nil
		}
	case api.GetSpaces:
		if pv, ok := v.(*api.GetSpaces); ok {
			*pv = mm
			return nil
		}
	case *api.Peek:
		if pv, ok := v.(*api.Peek); ok {
			*pv = *mm
			return nil
		}
	case api.Peek:
		if pv, ok := v.(*api.Peek); ok {
			*pv = mm
			return nil
		}
	case *api.ConsumeSegment:
		if pv, ok := v.(*api.ConsumeSegment); ok {
			*pv = *mm
			return nil
		}
	case api.ConsumeSegment:
		if pv, ok := v.(*api.ConsumeSegment); ok {
			*pv = mm
			return nil
		}
	case *api.SubscribeToSegmentStatus:
		if pv, ok := v.(*api.SubscribeToSegmentStatus); ok {
			*pv = *mm
			return nil
		}
	case api.SubscribeToSegmentStatus:
		if pv, ok := v.(*api.SubscribeToSegmentStatus); ok {
			*pv = mm
			return nil
		}
	case *api.SegmentNotification:
		if pv, ok := v.(*api.SegmentNotification); ok {
			*pv = *mm
			return nil
		}
	case api.SegmentNotification:
		if pv, ok := v.(*api.SegmentNotification); ok {
			*pv = mm
			return nil
		}
	}
	return fmt.Errorf("not-handled")
}

// reflectFallbackAssign attempts to assign msg to v using reflection. Returns
// nil on success, non-nil if not handled.
func reflectFallbackAssign(msg any, v any) error {
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr && !val.IsNil() {
		mv := reflect.ValueOf(msg)
		if mv.IsValid() {
			if mv.Type().AssignableTo(val.Elem().Type()) {
				val.Elem().Set(mv)
				return nil
			}
			if mv.Kind() == reflect.Ptr && mv.Elem().IsValid() && mv.Elem().Type().AssignableTo(val.Elem().Type()) {
				val.Elem().Set(mv.Elem())
				return nil
			}
			if mv.Type().ConvertibleTo(val.Elem().Type()) {
				val.Elem().Set(mv.Convert(val.Elem().Type()))
				return nil
			}
			if mv.Kind() == reflect.Ptr && mv.Elem().IsValid() && mv.Elem().Type().ConvertibleTo(val.Elem().Type()) {
				val.Elem().Set(mv.Elem().Convert(val.Elem().Type()))
				return nil
			}
		}
	}
	return fmt.Errorf("not-handled")
}

func (s *InProcBidiStream) CloseSend(err error) error {
	s.closeSend(err)
	return s.closeErr
}

func (s *InProcBidiStream) Close(err error) {
	s.closeSend(err)
	s.closeOnce.Do(func() {
		close(s.closed)
	})
}

func (s *InProcBidiStream) Closed() <-chan struct{} {
	return s.closed
}

func (s *InProcBidiStream) EndOfStreamError() error {
	return io.EOF
}

func LinkStreams(client, server *InProcBidiStream) {
	// Wire the server's recv channel to the client's send channel and vice-versa.
	// This avoids creating forwarding goroutines and an extra channel hop per message.
	server.recvChan = client.sendChan
	server.recvOwned = false
	client.recvChan = server.sendChan
	client.recvOwned = false
}

func (s *InProcBidiStream) closeSend(err error) {
	var didClose bool
	s.sendClosed.Do(func() {
		s.closeErr = err
		didClose = true
		close(s.sendChan)
	})
	// If a later caller attempts to close with a non-nil error, prefer the non-nil error
	// over a previously set nil to ensure remote-side errors are observable locally.
	if !didClose && err != nil {
		s.sendCloseMu.Lock()
		if s.closeErr == nil {
			s.closeErr = err
		}
		s.sendCloseMu.Unlock()
	}
}

type ErrorMessage struct {
	Type string `json:"type"`
	Err  string `json:"err,omitempty"`
}
