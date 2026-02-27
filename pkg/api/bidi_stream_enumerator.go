package api

import (
	"errors"
	"io"
	"log/slog"

	"github.com/fgrzl/enumerators"
)

// BidiStreamEnumerator provides an enumerator interface over a bidirectional stream.
type BidiStreamEnumerator[T any] struct {
	stream  BidiStream
	end     error
	current *T
	err     error
}

func (e *BidiStreamEnumerator[T]) MoveNext() bool {
	if e.stream == nil {
		return false
	}
	var current T

	err := e.stream.Decode(&current)
	if err == io.EOF || errors.Is(err, e.stream.EndOfStreamError()) {
		e.current = nil
		return false
	}

	if err != nil {
		slog.Warn("BidiStreamEnumerator: decode error - will propagate", "err", err)
		e.err = err
		return false
	}
	e.current = &current
	return true
}

func (e *BidiStreamEnumerator[T]) Current() (T, error) {
	if e.current == nil {
		var zero T
		return zero, e.err
	}
	return *e.current, e.err
}

func (e *BidiStreamEnumerator[T]) Err() error {
	return e.err
}

func (e *BidiStreamEnumerator[T]) Dispose() {
	if e.stream != nil {
		e.stream.Close(e.Err())
	}
}

// NewStreamEnumerator creates a new enumerator that reads responses from a bidirectional stream.
// If stream is nil, returns a degenerate enumerator that never yields and Err() returns an error.
func NewStreamEnumerator[T any](stream BidiStream) enumerators.Enumerator[T] {
	if stream == nil {
		return &BidiStreamEnumerator[T]{err: errors.New("streamkit: nil stream")}
	}
	return &BidiStreamEnumerator[T]{stream: stream, end: stream.EndOfStreamError()}
}
