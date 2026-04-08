package inprockit

import (
	"io"
	"sync"
	"testing"
	"time"

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

func TestShouldReturnClosedPipeWhenEncodingAfterCloseSend(t *testing.T) {
	stream := NewInProcBidiStream()

	require.NoError(t, stream.CloseSend(nil))
	require.ErrorIs(t, stream.Encode("late message"), io.ErrClosedPipe)

	select {
	case <-stream.Closed():
		t.Fatal("CloseSend should not fully close the stream")
	default:
	}
}

func TestShouldStillDecodePeerMessagesAfterLocalCloseSend(t *testing.T) {
	client := NewInProcBidiStream()
	server := NewInProcBidiStream()
	LinkStreams(client, server)

	require.NoError(t, client.CloseSend(nil))
	require.NoError(t, server.Encode("reply"))

	var out string
	require.NoError(t, client.Decode(&out))
	require.Equal(t, "reply", out)
	require.ErrorIs(t, client.Encode("late message"), io.ErrClosedPipe)

	select {
	case <-client.Closed():
		t.Fatal("CloseSend should leave the receive side open")
	default:
	}
}

func TestShouldPreferNonNilCloseErrorAcrossRepeatedCloseSendCalls(t *testing.T) {
	stream := NewInProcBidiStream()

	require.NoError(t, stream.CloseSend(nil))
	require.ErrorIs(t, stream.CloseSend(io.ErrUnexpectedEOF), io.ErrUnexpectedEOF)
}

func TestShouldNotPanicWhenEncodeRacesWithCloseSend(t *testing.T) {
	for iteration := 0; iteration < 256; iteration++ {
		stream := NewInProcBidiStream()
		start := make(chan struct{})
		panicCh := make(chan any, 2)
		encodeErrCh := make(chan error, 1)
		closeErrCh := make(chan error, 1)

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			defer func() {
				if recovered := recover(); recovered != nil {
					panicCh <- recovered
				}
			}()

			<-start
			encodeErrCh <- stream.Encode("message")
		}()

		go func() {
			defer wg.Done()
			defer func() {
				if recovered := recover(); recovered != nil {
					panicCh <- recovered
				}
			}()

			<-start
			closeErrCh <- stream.CloseSend(nil)
		}()

		close(start)
		wg.Wait()
		close(panicCh)

		for recovered := range panicCh {
			t.Fatalf("iteration %d panicked: %v", iteration, recovered)
		}

		require.NoError(t, <-closeErrCh)
		encodeErr := <-encodeErrCh
		if encodeErr != nil {
			require.ErrorIs(t, encodeErr, io.ErrClosedPipe)
		}
		require.ErrorIs(t, stream.Encode("late message"), io.ErrClosedPipe)
	}
}

func TestShouldUnblockDecodeWhenClosedLocally(t *testing.T) {
	stream := NewInProcBidiStream()
	errCh := make(chan error, 1)

	go func() {
		var out string
		errCh <- stream.Decode(&out)
	}()

	stream.Close(nil)

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for decode to unblock after close")
	}
}

func TestShouldSignalPeerClosedWhenLocalStreamCloses(t *testing.T) {
	client := NewInProcBidiStream()
	server := NewInProcBidiStream()
	LinkStreams(client, server)

	client.Close(nil)

	select {
	case <-server.Closed():
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for peer closed signal")
	}

	require.ErrorIs(t, server.Encode("late message"), io.ErrClosedPipe)
}

func TestShouldNotDeadlockWhenBothLinkedStreamsClose(t *testing.T) {
	client := NewInProcBidiStream()
	server := NewInProcBidiStream()
	LinkStreams(client, server)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		client.Close(nil)
	}()
	go func() {
		defer wg.Done()
		server.Close(nil)
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for simultaneous linked stream closes")
	}

	select {
	case <-client.Closed():
	default:
		t.Fatal("client stream should be closed")
	}
	select {
	case <-server.Closed():
	default:
		t.Fatal("server stream should be closed")
	}
}
