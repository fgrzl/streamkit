package wskit

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestRegisterCreatesChannel(t *testing.T) {
	t.Run("Given a muxer", func(t *testing.T) {
		m := &WebSocketMuxer{
			Context:  context.Background(),
			name:     "client",
			channels: make(map[uuid.UUID]*MuxerBidiStream),
		}

		t.Run("When a channel is registered", func(t *testing.T) {
			storeID := uuid.New()
			channelID := uuid.New()

			bidi := m.register(storeID, channelID)
			assert.NotNil(t, bidi)

			m.channelsMu.RLock()
			_, ok := m.channels[channelID]
			m.channelsMu.RUnlock()
			assert.True(t, ok, "expected channel to be registered in muxer")

			// Offer a JSON payload (as []byte) and decode it using Decode to ensure delivery
			payload := []byte(`"hello"`)
			ok = bidi.Offer(payload)
			assert.True(t, ok, "expected Offer to succeed")

			var v string
			assert.NoError(t, bidi.Decode(&v))
			assert.Equal(t, "hello", v)
		})
	})
}

func TestRegisterCleanupRemovesChannel(t *testing.T) {
	t.Run("Given a muxer with a registered channel", func(t *testing.T) {
		m := &WebSocketMuxer{
			Context:  context.Background(),
			name:     "client",
			channels: make(map[uuid.UUID]*MuxerBidiStream),
		}

		storeID := uuid.New()
		channelID := uuid.New()

		bidi := m.register(storeID, channelID)

		t.Run("When onClose is invoked", func(t *testing.T) {
			if bidi.onClose != nil {
				bidi.onClose()
			}

			m.channelsMu.RLock()
			_, ok := m.channels[channelID]
			m.channelsMu.RUnlock()
			assert.False(t, ok, "expected channel to be removed after onClose")
		})
	})
}

func TestRegisterOverwrite(t *testing.T) {
	t.Run("Given a muxer", func(t *testing.T) {
		m := &WebSocketMuxer{
			Context:  context.Background(),
			name:     "client",
			channels: make(map[uuid.UUID]*MuxerBidiStream),
		}

		t.Run("When the same channelID is registered twice", func(t *testing.T) {
			storeID := uuid.New()
			channelID := uuid.New()

			first := m.register(storeID, channelID)
			second := m.register(storeID, channelID)

			assert.NotEqual(t, first, second)

			m.channelsMu.RLock()
			got := m.channels[channelID]
			m.channelsMu.RUnlock()
			assert.Equal(t, second, got)
		})
	})
}

func TestOfferAfterClose(t *testing.T) {
	t.Run("Given a closed stream", func(t *testing.T) {
		m := &WebSocketMuxer{
			Context:  context.Background(),
			name:     "client",
			channels: make(map[uuid.UUID]*MuxerBidiStream),
		}

		storeID := uuid.New()
		channelID := uuid.New()

		bidi := m.register(storeID, channelID)
		// Mark the stream closed without invoking CloseSend (which would use websocket)
		atomic.StoreUint32(&bidi.closedFlag, 1)
		select {
		case <-bidi.closed:
		default:
			close(bidi.closed)
		}

		ok := bidi.Offer([]byte(`"x"`))
		assert.False(t, ok)
	})
}
