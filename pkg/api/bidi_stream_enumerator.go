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
	var current T

	err := e.stream.Decode(&current)
	if err == io.EOF || errors.Is(err, e.stream.EndOfStreamError()) {
		slog.Debug("BidiStreamEnumerator: stream ended (EOF)", "err", err)
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
	return *e.current, e.err
}

func (e *BidiStreamEnumerator[T]) Err() error {
	return e.err
}

func (e *BidiStreamEnumerator[T]) Dispose() {
	e.stream.Close(e.Err())
}

// NewStreamEnumerator creates a new enumerator that reads responses from a bidirectional stream.
func NewStreamEnumerator[T any](stream BidiStream) enumerators.Enumerator[T] {
	return &BidiStreamEnumerator[T]{stream: stream, end: stream.EndOfStreamError()}
}
