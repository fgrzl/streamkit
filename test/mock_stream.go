package test

import (
	"fmt"
	"reflect"
	"sync"
)

// MockBidiStream is a mock implementation of BidiStream.
type MockBidiStream struct {
	sendChan   chan any
	recvChan   chan any
	sendClosed sync.Once
	rcvClosed  sync.Once
	closeErr   error
}

// NewMockBidiStream creates a new MockBidiStream.
func NewMockBidiStream() *MockBidiStream {
	return &MockBidiStream{
		sendChan: make(chan any, 10_000),
		recvChan: make(chan any, 10_000),
	}
}

// Encode sends a message to the other side.
func (s *MockBidiStream) Encode(m any) error {
	if s.closeErr != nil {
		return s.closeErr
	}
	s.sendChan <- m
	return nil
}

func (s *MockBidiStream) Decode(v any) error {
	msg, ok := <-s.recvChan
	if !ok {
		return s.EndOfStreamError()
	}

	// Ensure v is a pointer
	vValue := reflect.ValueOf(v)
	if vValue.Kind() != reflect.Ptr {
		return fmt.Errorf("Decode: expected pointer, got %T", v)
	}

	// Get the actual type that v points to
	vElem := vValue.Elem()
	msgValue := reflect.ValueOf(msg)

	// Ensure assignability
	if !msgValue.Type().AssignableTo(vElem.Type()) {
		return fmt.Errorf("type mismatch: expected %T, got %T", vElem.Interface(), msg)
	}

	// Set the value
	vElem.Set(msgValue)
	return nil
}

// CloseSend closes the sending side of the stream.
func (s *MockBidiStream) CloseSend(err error) error {
	return s.safeCloseSend(err)
}

// Close closes the entire stream.
func (s *MockBidiStream) Close(err error) {
	s.safeCloseSend(err)
	s.safeCloseRcv()
}

// EndOfStreamError returns the error when the stream ends.
func (s *MockBidiStream) EndOfStreamError() error {
	return EndOfStreamError{}
}

func LinkStreams(client, server *MockBidiStream) {
	go func() {
		defer server.safeCloseRcv()
		for msg := range client.sendChan {
			if !safeSend(server.recvChan, msg) {
				return // Exit loop if send fails
			}
		}
	}()

	go func() {
		defer client.safeCloseRcv()
		for msg := range server.sendChan {
			if !safeSend(client.recvChan, msg) {
				return // Exit loop if send fails
			}
		}
	}()
}

// safeSend attempts to send a message and prevents panicking on a closed channel
func safeSend[T any](ch chan T, msg T) bool {
	defer func() {
		if r := recover(); r != nil {
			// Channel is closed, prevent further sends
		}
	}()
	select {
	case ch <- msg:
		return true
	default:
		return false
	}
}

func (s *MockBidiStream) safeCloseSend(err error) error {
	s.sendClosed.Do(func() {
		s.closeErr = err
		close(s.sendChan)
	})
	return err
}

func (s *MockBidiStream) safeCloseRcv() {
	s.rcvClosed.Do(func() {
		close(s.recvChan)
	})
}

type EndOfStreamError struct{}

func (e EndOfStreamError) Error() string {
	return "end of stream"
}
