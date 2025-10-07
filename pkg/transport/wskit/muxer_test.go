package wskit

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/websocket"
)

func TestShouldRegisterChannelInMuxer(t *testing.T) {
	// Arrange
	m := &WebSocketMuxer{
		Context:  context.Background(),
		name:     "client",
		channels: make(map[uuid.UUID]*MuxerBidiStream),
	}

	// Act
	storeID := uuid.New()
	channelID := uuid.New()
	bidi := m.register(storeID, channelID)

	// Assert
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
}

func TestShouldRemoveChannelOnOnClose(t *testing.T) {
	// Arrange
	m := &WebSocketMuxer{
		Context:  context.Background(),
		name:     "client",
		channels: make(map[uuid.UUID]*MuxerBidiStream),
	}
	storeID := uuid.New()
	channelID := uuid.New()
	bidi := m.register(storeID, channelID)

	// Act
	if bidi.onClose != nil {
		bidi.onClose()
	}

	// Assert
	m.channelsMu.RLock()
	_, ok := m.channels[channelID]
	m.channelsMu.RUnlock()
	assert.False(t, ok, "expected channel to be removed after onClose")
}

func TestShouldOverwriteExistingRegistration(t *testing.T) {
	// Arrange
	m := &WebSocketMuxer{
		Context:  context.Background(),
		name:     "client",
		channels: make(map[uuid.UUID]*MuxerBidiStream),
	}

	// Act
	storeID := uuid.New()
	channelID := uuid.New()
	first := m.register(storeID, channelID)
	second := m.register(storeID, channelID)

	// Assert
	assert.NotEqual(t, first, second)
	m.channelsMu.RLock()
	got := m.channels[channelID]
	m.channelsMu.RUnlock()
	assert.Equal(t, second, got)
}

func TestShouldRejectOfferAfterClose(t *testing.T) {
	// Arrange
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

	// Act
	ok := bidi.Offer([]byte(`"x"`))

	// Assert
	assert.False(t, ok)
}

func TestRegisterStoresAndCleanupRemovesChannel(t *testing.T) {
	// Arrange: create a lightweight muxer with initialized channels map
	m := &WebSocketMuxer{
		Context:  context.Background(),
		channels: make(map[uuid.UUID]*MuxerBidiStream),
		done:     make(chan struct{}),
	}

	storeID := uuid.New()
	channelID := uuid.New()

	// Act: register a channel
	bidi := m.register(storeID, channelID)

	// Assert: channel present
	require.NotNil(t, bidi)
	m.channelsMu.RLock()
	_, exists := m.channels[channelID]
	m.channelsMu.RUnlock()
	assert.True(t, exists, "expected channel to be registered")

	// Act: close local stream which should invoke cleanup
	bidi.CloseLocal(nil)

	// allow onClose propagation
	time.Sleep(5 * time.Millisecond)

	// Assert: channel removed
	m.channelsMu.RLock()
	_, exists = m.channels[channelID]
	m.channelsMu.RUnlock()
	assert.False(t, exists, "expected channel to be removed after CloseLocal")
}

func TestHeartbeatReturnsImmediatelyWhenDisabled(t *testing.T) {
	m := &WebSocketMuxer{
		Context:       context.Background(),
		pingInterval:  0,
		heartbeatStop: make(chan struct{}),
	}

	// Should return immediately and not block
	done := make(chan struct{})
	go func() {
		m.heartbeat()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("heartbeat did not return immediately when pingInterval=0")
	}
}

func TestShouldShutdownOnSendDataError(t *testing.T) {
	// Arrange
	m := &WebSocketMuxer{
		Context:  context.Background(),
		channels: make(map[uuid.UUID]*MuxerBidiStream),
		done:     make(chan struct{}),
	}

	// simulate send failing
	sendCalled := int32(0)
	m.sendJSON = func(conn *websocket.Conn, v interface{}) error {
		atomic.AddInt32(&sendCalled, 1)
		return errors.New("boom")
	}

	// Act
	storeID := uuid.New()
	channelID := uuid.New()
	err := m.sendData(storeID, channelID, []byte(`"x"`))

	// Assert
	require.Error(t, err)
	// sendJSON was attempted
	assert.Equal(t, int32(1), atomic.LoadInt32(&sendCalled))

	// shutdown should have closed done
	select {
	case <-m.done:
		// success
	default:
		t.Fatalf("expected muxer to be shutdown after send error")
	}
}

func TestShouldSendAccessDeniedError(t *testing.T) {
	// Arrange
	m := &WebSocketMuxer{
		Context:  context.Background(),
		channels: make(map[uuid.UUID]*MuxerBidiStream),
		done:     make(chan struct{}),
		// session that denies access
		session: &muxerSession{allowAll: false, allowedStores: map[uuid.UUID]struct{}{}},
	}

	var captured MuxerMsg
	sendCalled := 0
	m.sendJSON = func(conn *websocket.Conn, v interface{}) error {
		if msg, ok := v.(*MuxerMsg); ok {
			captured = *msg
			sendCalled++
			return nil
		}
		return nil
	}

	// Act
	storeID := uuid.New()
	channelID := uuid.New()
	m.processMessage(&MuxerMsg{ControlType: ControlTypeData, StoreID: storeID, ChannelID: channelID, Payload: []byte(`"hi"`)})

	// Assert: a control error was sent back
	require.Equal(t, 1, sendCalled)
	assert.Equal(t, ControlTypeError, captured.ControlType)
	assert.Equal(t, storeID, captured.StoreID)
	assert.Equal(t, channelID, captured.ChannelID)

	var em ErrorMessage
	require.NoError(t, json.Unmarshal(captured.Payload, &em))
	assert.Equal(t, "access denied", em.Err)
}
