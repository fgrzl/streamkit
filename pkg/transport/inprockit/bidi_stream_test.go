package inprockit

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldEncodeDecodeRoundtripOnInProcBidiStreamLoopback(t *testing.T) {
	// Arrange
	stream := NewInProcBidiStreamLoopback()
	type msg struct{ Text string }

	// Act
	go func() {
		_ = stream.Encode(msg{Text: "hello"})
		stream.Close(nil)
	}()
	var out msg
	err := stream.Decode(&out)

	// Assert
	require.NoError(t, err)
	require.Equal(t, "hello", out.Text)
}

func TestShouldReturnEOFWhenDecodingFromClosedLoopback(t *testing.T) {
	// Arrange
	stream := NewInProcBidiStreamLoopback()
	stream.Close(nil)

	// Act
	var out string
	err := stream.Decode(&out)

	// Assert
	require.ErrorIs(t, err, io.EOF)
}

func TestShouldSurfaceRemoteErrorMessagesOnDecode(t *testing.T) {
	// Arrange
	stream := NewInProcBidiStreamLoopback()

	// Act
	go func() {
		_ = stream.Encode(ErrorMessage{Type: "error", Err: "fail"})
		stream.Close(nil)
	}()
	var out string
	err := stream.Decode(&out)

	// Assert
	require.ErrorContains(t, err, "remote error: fail")
}

func TestShouldCloseSendAndSignalClosed(t *testing.T) {
	// Arrange
	stream := NewInProcBidiStream()

	// Act
	stream.CloseSend(io.ErrClosedPipe)

	// Assert
	require.Equal(t, io.ErrClosedPipe, stream.CloseSend(nil))
	stream.Close(nil)
	select {
	case <-stream.Closed():
		// closed as expected
	default:
		require.Fail(t, "stream should be closed")
	}
}
