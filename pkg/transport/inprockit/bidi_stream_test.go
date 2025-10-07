package inprockit

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInProcBidiStream_EncodeDecode_Roundtrip(t *testing.T) {
	stream := NewInProcBidiStream()
	type msg struct{ Text string }
	go func() {
		_ = stream.Encode(msg{Text: "hello"})
		stream.Close(nil)
	}()
	var out msg
	err := stream.Decode(&out)
	require.NoError(t, err)
	require.Equal(t, "hello", out.Text)
}

func TestInProcBidiStream_EndOfStreamError(t *testing.T) {
	stream := NewInProcBidiStream()
	stream.Close(nil)
	var out string
	err := stream.Decode(&out)
	require.ErrorIs(t, err, io.EOF)
}

func TestInProcBidiStream_ErrorMessageHandling(t *testing.T) {
	stream := NewInProcBidiStream()
	go func() {
		_ = stream.Encode(ErrorMessage{Type: "error", Err: "fail"})
		stream.Close(nil)
	}()
	var out string
	err := stream.Decode(&out)
	require.ErrorContains(t, err, "remote error: fail")
}

func TestInProcBidiStream_CloseSendAndClosed(t *testing.T) {
	stream := NewInProcBidiStream()
	stream.CloseSend(io.ErrClosedPipe)
	require.Equal(t, io.ErrClosedPipe, stream.CloseSend(nil))
	stream.Close(nil)
	select {
	case <-stream.Closed():
		// closed as expected
	default:
		t.Fatalf("stream should be closed")
	}
}
