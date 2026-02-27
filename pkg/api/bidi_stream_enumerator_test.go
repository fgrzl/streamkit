package api

import (
	"errors"
	"io"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeBidiStream is a minimal BidiStream for testing the enumerator.
type fakeBidiStream struct {
	decodeFn    func(any) error
	closeSendFn func(error) error
	closeFn     func(error)
}

func (f *fakeBidiStream) Encode(m any) error { return nil }
func (f *fakeBidiStream) Decode(v any) error {
	if f.decodeFn != nil {
		return f.decodeFn(v)
	}
	return io.EOF
}
func (f *fakeBidiStream) CloseSend(err error) error {
	if f.closeSendFn != nil {
		return f.closeSendFn(err)
	}
	return nil
}
func (f *fakeBidiStream) Close(err error) {
	if f.closeFn != nil {
		f.closeFn(err)
	}
}
func (f *fakeBidiStream) EndOfStreamError() error { return io.EOF }
func (f *fakeBidiStream) Closed() <-chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}

func TestShouldReturnDegenerateEnumeratorWhenStreamIsNil(t *testing.T) {
	// Arrange
	// (no setup)

	// Act
	enum := NewStreamEnumerator[*SegmentStatus](nil)

	// Assert
	require.NotNil(t, enum)
	assert.False(t, enum.MoveNext())
	_, err := enum.Current()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil stream")
	assert.Error(t, enum.Err())
	enum.Dispose() // no panic
}

func TestShouldYieldItemsAndCloseStreamWhenMoveNextAndDisposeCalled(t *testing.T) {
	// Arrange
	closed := false
	stream := &fakeBidiStream{
		decodeFn: func(v any) error {
			if ptr, ok := v.(**SegmentStatus); ok {
				*ptr = &SegmentStatus{LastSequence: 1}
				return nil
			}
			return io.EOF
		},
		closeFn: func(error) { closed = true },
	}
	enum := NewStreamEnumerator[*SegmentStatus](stream)
	require.NotNil(t, enum)

	// Act
	gotNext := enum.MoveNext()
	cur, err := enum.Current()
	require.NoError(t, err)
	stream.decodeFn = func(v any) error { return io.EOF }
	gotNextAfterEOF := enum.MoveNext()
	enum.Dispose()

	// Assert
	assert.True(t, gotNext)
	assert.NotNil(t, cur)
	assert.Equal(t, uint64(1), cur.LastSequence)
	assert.False(t, gotNextAfterEOF)
	assert.True(t, closed)
}

func TestShouldPropagateDecodeErrorWhenDecodeReturnsError(t *testing.T) {
	// Arrange
	stream := &fakeBidiStream{
		decodeFn: func(any) error { return io.ErrClosedPipe },
	}
	enum := NewStreamEnumerator[*SegmentStatus](stream)

	// Act
	gotNext := enum.MoveNext()
	err := enum.Err()

	// Assert
	assert.False(t, gotNext)
	assert.Error(t, err)
}

func TestShouldCollectAllItemsWhenToSliceCalledOnEnumerator(t *testing.T) {
	// Arrange
	calls := 0
	stream := &fakeBidiStream{
		decodeFn: func(v any) error {
			if ptr, ok := v.(**SegmentStatus); ok {
				calls++
				*ptr = &SegmentStatus{LastSequence: uint64(calls)}
				if calls > 3 {
					return io.EOF
				}
				return nil
			}
			return io.EOF
		},
	}
	enum := NewStreamEnumerator[*SegmentStatus](stream)

	// Act
	slice, err := enumerators.ToSlice(enum)

	// Assert
	require.NoError(t, err)
	require.Len(t, slice, 3)
	assert.Equal(t, uint64(1), slice[0].LastSequence)
	assert.Equal(t, uint64(2), slice[1].LastSequence)
	assert.Equal(t, uint64(3), slice[2].LastSequence)
}

func TestShouldReturnZeroAndErrWhenCurrentCalledBeforeAnyMoveNext(t *testing.T) {
	// Arrange
	stream := &fakeBidiStream{decodeFn: func(any) error { return io.EOF }}
	enum := NewStreamEnumerator[*SegmentStatus](stream)

	// Act
	cur, err := enum.Current()

	// Assert
	assert.Nil(t, cur)
	assert.NoError(t, err)
}

func TestShouldTreatCustomEndOfStreamErrorAsEndWhenDecodeReturnsMatchingError(t *testing.T) {
	// Arrange
	var customEOS = errors.New("end of stream")
	stream := &fakeBidiStream{
		decodeFn: func(v any) error {
			if _, ok := v.(**SegmentStatus); ok {
				return customEOS
			}
			return io.EOF
		},
	}
	// Override EndOfStreamError so errors.Is(err, EndOfStreamError()) is true
	streamWithEOS := &fakeBidiStreamWithEOS{fakeBidiStream: stream, eos: customEOS}
	enum := NewStreamEnumerator[*SegmentStatus](streamWithEOS)

	// Act
	gotNext := enum.MoveNext()
	err := enum.Err()

	// Assert
	assert.False(t, gotNext)
	assert.NoError(t, err) // EOS is not stored as err
}

func TestShouldPassErrToStreamCloseWhenDisposeCalledAfterDecodeError(t *testing.T) {
	// Arrange
	closedWithErr := error(nil)
	stream := &fakeBidiStream{
		decodeFn: func(any) error { return io.ErrClosedPipe },
		closeFn:  func(err error) { closedWithErr = err },
	}
	enum := NewStreamEnumerator[*SegmentStatus](stream)
	_ = enum.MoveNext()

	// Act
	enum.Dispose()

	// Assert
	assert.ErrorIs(t, closedWithErr, io.ErrClosedPipe)
}

// fakeBidiStreamWithEOS extends fakeBidiStream with a custom EndOfStreamError for testing.
type fakeBidiStreamWithEOS struct {
	*fakeBidiStream
	eos error
}

func (f *fakeBidiStreamWithEOS) EndOfStreamError() error { return f.eos }
