package wskit

import (
	"encoding/json"
	"io"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helper to create a stream with a capturing encoder
func newCapturingStream() (*MuxerBidiStream, *[]byte) {
	var last []byte
	enc := func(p []byte) error {
		last = append([]byte(nil), p...)
		return nil
	}
	s := NewMuxerBidiStream(enc, nil)
	return s, &last
}

func TestEncodeUsesEncoder(t *testing.T) {
	t.Run("Given a stream with a capturing encoder", func(t *testing.T) {
		s, got := newCapturingStream()
		assert.NoError(t, s.Encode(map[string]string{"a": "b"}))

		var m map[string]string
		assert.NoError(t, json.Unmarshal(*got, &m))
		assert.Equal(t, "b", m["a"])
	})
}

func TestDecodePayloadTypes(t *testing.T) {
	t.Run("Given a stream", func(t *testing.T) {
		s, _ := newCapturingStream()

		t.Run("Decode []byte payload", func(t *testing.T) {
			s.RecvChan() <- []byte("\"one\"")
			var a string
			assert.NoError(t, s.Decode(&a))
			assert.Equal(t, "one", a)
		})

		t.Run("Decode string payload", func(t *testing.T) {
			s.RecvChan() <- "\"two\""
			var b string
			assert.NoError(t, s.Decode(&b))
			assert.Equal(t, "two", b)
		})

		t.Run("Decode non-bytes value", func(t *testing.T) {
			s.RecvChan() <- map[string]string{"k": "v"}
			var mm map[string]string
			assert.NoError(t, s.Decode(&mm))
			assert.Equal(t, "v", mm["k"])
		})
	})
}

func TestDecodeErrorMessageTypes(t *testing.T) {
	t.Run("Given ErrorMessage payloads", func(t *testing.T) {
		s, _ := newCapturingStream()

		t.Run("remote closed with error", func(t *testing.T) {
			s.RecvChan() <- []byte(`{"type":"mux::close","err":"boom"}`)
			var v string
			assert.EqualError(t, s.Decode(&v), "remote closed stream: boom")
		})

		t.Run("remote closed without error", func(t *testing.T) {
			s.RecvChan() <- []byte(`{"type":"mux::close"}`)
			var v string
			assert.Equal(t, io.EOF, s.Decode(&v))
		})

		t.Run("remote error", func(t *testing.T) {
			s.RecvChan() <- []byte(`{"type":"mux::error","err":"oops"}`)
			var v string
			assert.EqualError(t, s.Decode(&v), "remote error: oops")
		})

		t.Run("unknown error type", func(t *testing.T) {
			s.RecvChan() <- []byte(`{"type":"mux::weird","err":"x"}`)
			var v string
			assert.Error(t, s.Decode(&v))
		})
	})
}

func TestCloseSendEncodesCloseMessage(t *testing.T) {
	t.Run("CloseSend encodes close message", func(t *testing.T) {
		s, got := newCapturingStream()
		assert.NoError(t, s.CloseSend(nil))
		var em ErrorMessage
		assert.NoError(t, json.Unmarshal(*got, &em))
		assert.Equal(t, controlMsgSentinel+"close", em.Type)

		// with error
		assert.NoError(t, s.CloseSend(io.ErrUnexpectedEOF))
		var em2 ErrorMessage
		assert.NoError(t, json.Unmarshal(*got, &em2))
		assert.NotEmpty(t, em2.Err)
	})
}

func TestCloseInvokesOnCloseAndIsIdempotent(t *testing.T) {
	t.Run("Close should call onClose once and be idempotent", func(t *testing.T) {
		called := 0
		encCalled := 0
		enc := func(p []byte) error {
			encCalled++
			return nil
		}
		onClose := func() { called++ }
		s := NewMuxerBidiStream(enc, onClose)

		// Act
		s.Close(nil)

		// Assert
		assert.Equal(t, 1, called)
		assert.True(t, s.IsClosed())
		select {
		case <-s.Closed():
		default:
			require.Fail(t, "expected Closed channel to be closed")
		}

		// Act: idempotent close
		s.Close(nil)
		assert.Equal(t, 1, called)
		require.Greater(t, encCalled, 0, "expected encoder to be called during Close")
	})
}

func TestRecvChanAndOfferBehavior(t *testing.T) {
	t.Run("RecvChan send should be readable and Offer should fail when closed", func(t *testing.T) {
		s, _ := newCapturingStream()

		s.RecvChan() <- []byte("\"ping\"")
		var v string
		assert.NoError(t, s.Decode(&v))
		assert.Equal(t, "ping", v)

		atomic.StoreUint32(&s.closedFlag, 1)
		select {
		case <-s.closed:
		default:
			close(s.closed)
		}
		assert.False(t, s.Offer([]byte("x")))
	})
}

func TestEndOfStreamError(t *testing.T) {
	t.Run("EndOfStreamError returns io.EOF", func(t *testing.T) {
		s, _ := newCapturingStream()
		assert.Equal(t, io.EOF, s.EndOfStreamError())
	})
}

func TestCloseLocalDrainsBufferedMessages(t *testing.T) {
	// Arrange
	s, _ := newCapturingStream()
	for i := 0; i < 5; i++ {
		ok := s.Offer([]byte(`"x"`))
		require.True(t, ok)
	}

	// Act
	s.CloseLocal(nil)

	// Assert: after CloseLocal the channel should be empty
	require.Equal(t, 0, len(s.recvChan), "expected recvChan to be drained by CloseLocal")
}

func TestDecodeIgnoresDomainObjectWithTypeField(t *testing.T) {
	// Arrange: a domain object that has a "type" field — previously consumed
	// as a control message, silently dropping the application payload.
	s, _ := newCapturingStream()
	s.Offer([]byte(`{"type":"close","data":"legit"}`))

	// Act
	var v map[string]string
	err := s.Decode(&v)

	// Assert: must be delivered to the application, not consumed as a close command
	require.NoError(t, err, "domain object with 'type' field must not be treated as a control message")
	assert.Equal(t, "legit", v["data"])
	assert.Equal(t, "close", v["type"])
}
