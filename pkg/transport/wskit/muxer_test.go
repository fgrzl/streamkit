package wskit

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/websocket"
)

func newQueueTestMuxer(queueSize int) *WebSocketMuxer {
	return &WebSocketMuxer{
		Context:        context.Background(),
		name:           "test",
		done:           make(chan struct{}),
		writeQueueSize: queueSize,
		writeQueue:     make(chan *MuxerMsg, queueSize),
		msgPool:        sync.Pool{New: func() any { return &MuxerMsg{} }},
		bufPool:        sync.Pool{New: func() any { return make([]byte, 0, 1024) }},
		writerDone:     make(chan struct{}),
		sendJSON:       func(_ *websocket.Conn, _ interface{}) error { return nil },
	}
}

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
	bidi := m.register(context.Background(), storeID, channelID)

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
	bidi := m.register(context.Background(), storeID, channelID)

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
	first := m.register(context.Background(), storeID, channelID)
	second := m.register(context.Background(), storeID, channelID)

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

	bidi := m.register(context.Background(), storeID, channelID)
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
	bidi := m.register(context.Background(), storeID, channelID)

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

func TestShouldShutdownOnHeartbeatTimeout(t *testing.T) {
	closed := make(chan struct{})
	m := &WebSocketMuxer{
		Context:       context.Background(),
		done:          make(chan struct{}),
		channels:      make(map[uuid.UUID]*MuxerBidiStream),
		heartbeatStop: make(chan struct{}),
		pongTimeout:   1,
	}

	stream := NewMuxerBidiStream(func([]byte) error { return nil }, func() {
		select {
		case <-closed:
		default:
			close(closed)
		}
	})
	channelID := uuid.New()
	m.channels[channelID] = stream
	atomic.StoreInt64(&m.lastPongUnix, 0)

	assert.True(t, m.checkHeartbeatTimeout())
	assert.Equal(t, int64(1), m.MissedPongs())

	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for stream to close after heartbeat timeout")
	}

	assert.True(t, stream.IsClosed())
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
	err := m.sendData(storeID, channelID, []byte(`"x"`), nil)

	// Assert
	require.Error(t, err)
	// sendJSON was attempted
	assert.Equal(t, int32(1), atomic.LoadInt32(&sendCalled))

	// shutdown runs asynchronously (go m.shutdown) to avoid deadlock; wait for done to close
	require.Eventually(t, func() bool {
		select {
		case <-m.done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond, "expected muxer to be shutdown after send error")
}

func TestShouldTrackWriteQueueDepthAcrossEnqueueAndDrain(t *testing.T) {
	m := newQueueTestMuxer(2)

	require.NoError(t, m.sendData(uuid.New(), uuid.New(), []byte("hello"), nil))
	assert.Equal(t, int64(1), m.WriteQueueDepth())

	go m.writePump()
	require.Eventually(t, func() bool {
		return m.WriteQueueDepth() == 0
	}, time.Second, 10*time.Millisecond)

	m.shutdown(nil)
	select {
	case <-m.writerDone:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for writer to stop")
	}
}

func TestShouldCountDataQueueFallbackWhenQueueIsFull(t *testing.T) {
	m := newQueueTestMuxer(1)

	require.NoError(t, m.sendData(uuid.New(), uuid.New(), []byte("first"), nil))
	require.NoError(t, m.sendData(uuid.New(), uuid.New(), []byte("second"), nil))

	assert.Equal(t, int64(1), m.WriteQueueDepth())
	assert.Equal(t, int64(1), m.WriteQueueFallbacks())
	assert.Equal(t, int64(0), m.WriteQueueSaturationWarnings())
}

func TestShouldCountControlQueueSaturationWarningsWhenFallbackPersists(t *testing.T) {
	m := newQueueTestMuxer(1)

	require.NoError(t, m.sendData(uuid.New(), uuid.New(), []byte("seed"), nil))
	for range writeQueueSaturationWarnThreshold {
		require.NoError(t, m.sendControl(ControlTypePing, uuid.New(), uuid.New(), nil))
	}

	assert.Equal(t, writeQueueSaturationWarnThreshold, m.WriteQueueFallbacks())
	assert.Equal(t, int64(1), m.WriteQueueSaturationWarnings())
	assert.Equal(t, int64(1), m.WriteQueueDepth())
}

func TestShouldCountPingQueueFallbackWhenQueueIsFull(t *testing.T) {
	m := newQueueTestMuxer(1)

	require.NoError(t, m.sendData(uuid.New(), uuid.New(), []byte("seed"), nil))
	require.NoError(t, m.sendPing())

	assert.Equal(t, int64(1), m.WriteQueueFallbacks())
	assert.Equal(t, int64(1), m.WriteQueueDepth())
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

func TestMuxerNoPanicOnConcurrentSendAndShutdown(t *testing.T) {
	m := &WebSocketMuxer{
		Context:        context.Background(),
		name:           "test",
		done:           make(chan struct{}),
		writeQueueSize: 256,
		writeQueue:     make(chan *MuxerMsg, 256),
		msgPool:        sync.Pool{New: func() any { return &MuxerMsg{} }},
		bufPool:        sync.Pool{New: func() any { return make([]byte, 0, 1024) }},
		writerDone:     make(chan struct{}),
		// dummy send that simulates a successful write
		sendJSON: func(conn *websocket.Conn, v interface{}) error { return nil },
	}

	// start write pump
	go m.writePump()

	var wg sync.WaitGroup
	nSenders := 50
	perSender := 200

	// start many concurrent senders
	for i := 0; i < nSenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perSender; j++ {
				// call various send paths; ignore errors
				_ = m.sendPing()
				_ = m.sendControl(ControlTypePing, uuid.Nil, uuid.Nil, nil)
				_ = m.sendData(uuid.Nil, uuid.Nil, []byte("hello"), nil)
				// small sleep to increase interleaving
				time.Sleep(time.Millisecond)
			}
		}()
	}

	// let senders run a short while then shutdown concurrently
	time.Sleep(10 * time.Millisecond)
	go m.shutdown(nil)

	// wait for senders to finish
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// senders finished
	case <-time.After(5 * time.Second):
		t.Fatal("senders did not finish in time")
	}

	// wait for writer to finish (writerDone should be closed by writePump defer)
	select {
	case <-m.writerDone:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not finish after shutdown")
	}
}

// TestRegisterOnClosedMuxerReturnsFailingStream verifies that calling Register
// on a muxer that has already been shut down returns a stream that immediately
// fails on Encode with ErrMuxerClosed, rather than creating an orphaned entry
// in the channels map.
func TestRegisterOnClosedMuxerReturnsFailingStream(t *testing.T) { // Arrange
	done := make(chan struct{})
	close(done) // muxer is already shut down
	m := &WebSocketMuxer{
		done:     done,
		channels: make(map[uuid.UUID]*MuxerBidiStream),
	}

	storeID := uuid.New()
	channelID := uuid.New()

	// Act
	bidi := m.Register(storeID, channelID)

	// Assert: stream should be returned (not nil) but Encode must fail immediately
	require.NotNil(t, bidi, "Register on closed muxer should return a non-nil stream")

	err := bidi.Encode([]byte("test"))
	assert.ErrorIs(t, err, ErrMuxerClosed, "Encode on stream from closed muxer should return ErrMuxerClosed")

	// The stream should NOT be in the channels map (no orphan)
	m.channelsMu.RLock()
	_, exists := m.channels[channelID]
	m.channelsMu.RUnlock()
	assert.False(t, exists, "Register on closed muxer should not add stream to channels map")
}

func TestShutdownWithOpenStreamDoesNotDeadlock(t *testing.T) {
	// Arrange: a muxer with a real registered stream whose cleanup acquires channelsMu.Lock.
	m := &WebSocketMuxer{
		Context:        context.Background(),
		name:           "test",
		done:           make(chan struct{}),
		channels:       make(map[uuid.UUID]*MuxerBidiStream),
		heartbeatStop:  make(chan struct{}),
		writeQueueSize: 16,
		writeQueue:     make(chan *MuxerMsg, 16),
		msgPool:        sync.Pool{New: func() any { return &MuxerMsg{} }},
		bufPool:        sync.Pool{New: func() any { return make([]byte, 0, 1024) }},
		writerDone:     make(chan struct{}),
		sendJSON:       func(_ *websocket.Conn, _ interface{}) error { return nil },
	}
	go m.writePump()
	// register via the normal path so the cleanup closure is the real one
	storeID := uuid.New()
	channelID := uuid.New()
	stream := m.register(context.Background(), storeID, channelID)
	require.NotNil(t, stream)

	// Act: shutdown must complete without deadlocking
	done := make(chan struct{})
	go func() {
		m.shutdown(nil)
		close(done)
	}()

	select {
	case <-done:
		// success: shutdown returned
	case <-time.After(time.Second):
		t.Fatal("shutdown deadlocked with an open stream")
	}

	// Assert: stream is closed and removed
	assert.True(t, stream.IsClosed(), "expected stream to be closed after shutdown")
	m.channelsMu.RLock()
	_, exists := m.channels[channelID]
	m.channelsMu.RUnlock()
	assert.False(t, exists, "expected channel to be removed after shutdown")
}
