package api

import (
	"errors"
	"io"

	"github.com/fgrzl/enumerators"
)

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
		e.current = nil
		return false
	}

	if err != nil {
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

// A new enumerator of the responses
func NewStreamEnumerator[T any](stream BidiStream) enumerators.Enumerator[T] {
	return &BidiStreamEnumerator[T]{stream: stream, end: stream.EndOfStreamError()}
}
