package mockkit

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sync"
)

type MockBidiStream struct {
	sendChan   chan any
	recvChan   chan any
	sendClosed sync.Once
	recvClosed sync.Once
	closeOnce  sync.Once
	closeErr   error
	closed     chan struct{}
}

func NewMockBidiStream() *MockBidiStream {
	return &MockBidiStream{
		sendChan: make(chan any, 10_000),
		recvChan: make(chan any, 10_000),
		closed:   make(chan struct{}),
	}
}

func (s *MockBidiStream) Encode(m any) error {
	if s.closeErr != nil {
		return s.closeErr
	}
	payload, err := json.Marshal(m)
	if err != nil {
		return err
	}
	select {
	case <-s.closed:
		return io.ErrClosedPipe
	case s.sendChan <- payload:
		return nil
	}
}

func (s *MockBidiStream) Decode(v any) error {
	msg, ok := <-s.recvChan
	if !ok {
		return s.EndOfStreamError()
	}

	var payload []byte
	switch m := msg.(type) {
	case []byte:
		payload = m
	case string:
		payload = []byte(m)
	default:
		return fmt.Errorf("invalid message type %T", m)
	}

	// Handle ErrorMessage types (same as MuxerBidiStream)
	var errMsg ErrorMessage
	if err := json.Unmarshal(payload, &errMsg); err == nil && errMsg.Type != "" {
		switch errMsg.Type {
		case "close":
			if errMsg.Err != "" {
				return fmt.Errorf("remote closed stream: %s", errMsg.Err)
			}
			return s.EndOfStreamError()
		case "error":
			return fmt.Errorf("remote error: %s", errMsg.Err)
		default:
			return fmt.Errorf("unknown error type %q: %s", errMsg.Type, errMsg.Err)
		}
	}

	// Decode into provided pointer
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return fmt.Errorf("Decode: expected non-nil pointer, got %T", v)
	}

	return json.Unmarshal(payload, v)
}

func (s *MockBidiStream) CloseSend(err error) error {
	s.closeSend(err)
	return s.closeErr
}

func (s *MockBidiStream) Close(err error) {
	s.closeSend(err)
	s.closeRecv()
	s.closeOnce.Do(func() {
		close(s.closed)
	})
}

func (s *MockBidiStream) Closed() <-chan struct{} {
	return s.closed
}

func (s *MockBidiStream) EndOfStreamError() error {
	return io.EOF
}

func LinkStreams(client, server *MockBidiStream) {
	go func() {
		defer server.closeRecv()
		for msg := range client.sendChan {
			server.recvChan <- msg
		}
	}()
	go func() {
		defer client.closeRecv()
		for msg := range server.sendChan {
			client.recvChan <- msg
		}
	}()
}

func (s *MockBidiStream) closeSend(err error) {
	s.sendClosed.Do(func() {
		s.closeErr = err
		close(s.sendChan)
	})
}

func (s *MockBidiStream) closeRecv() {
	s.recvClosed.Do(func() {
		close(s.recvChan)
	})
}

type ErrorMessage struct {
	Type string `json:"type"`
	Err  string `json:"err,omitempty"`
}
