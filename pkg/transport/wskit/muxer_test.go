package wskit

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/server"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/websocket"
)

func newQueueTestMuxer(queueSize int) *WebSocketMuxer {
	return &WebSocketMuxer{
		Context:                context.Background(),
		name:                   "test",
		done:                   make(chan struct{}),
		writeQueueSize:         queueSize,
		writeQueueOfferTimeout: defaultWriteQueueOfferTimeout,
		writeQueue:             make(chan *MuxerMsg, queueSize),
		msgPool:                sync.Pool{New: func() any { return &MuxerMsg{} }},
		bufPool:                sync.Pool{New: func() any { return make([]byte, 0, 1024) }},
		writerDone:             make(chan struct{}),
		sendJSON:               func(_ *websocket.Conn, _ interface{}) error { return nil },
	}
}

type fakeNode struct{}

func (f *fakeNode) Handle(context.Context, api.BidiStream) {}

func (f *fakeNode) Close() {}

type fakeNodeManager struct {
	getCalls atomic.Int32
}

func (f *fakeNodeManager) GetOrCreate(context.Context, uuid.UUID) (server.Node, error) {
	f.getCalls.Add(1)
	return &fakeNode{}, nil
}

func (f *fakeNodeManager) Remove(context.Context, uuid.UUID) {}

func (f *fakeNodeManager) Close() {}

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

func TestGetOrCreateStreamRejectsWhenStreamLimitExceeded(t *testing.T) {
	manager := &fakeNodeManager{}
	m := &WebSocketMuxer{
		Context:     context.Background(),
		name:        "server",
		done:        make(chan struct{}),
		channels:    make(map[uuid.UUID]*MuxerBidiStream),
		tombstones:  make(map[uuid.UUID]error),
		maxStreams:  1,
		nodeManager: manager,
		sendJSON: func(_ *websocket.Conn, _ interface{}) error {
			return nil
		},
	}
	atomic.StoreInt64(&m.activeStreams, 1)

	msg := &MuxerMsg{
		ControlType: ControlTypeData,
		StoreID:     uuid.New(),
		ChannelID:   uuid.New(),
		Payload:     []byte(`{}`),
	}

	bidi, err := m.getOrCreateStream(context.Background(), msg)

	require.Nil(t, bidi)
	require.ErrorIs(t, err, ErrTooManyStreams)
	assert.Equal(t, int32(0), manager.getCalls.Load(), "node manager should not be consulted once the stream cap is hit")
	m.channelsMu.RLock()
	defer m.channelsMu.RUnlock()
	assert.ErrorIs(t, m.tombstones[msg.ChannelID], ErrTooManyStreams)
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

func TestMuxerReceivePressureOptionsOverrideDefaults(t *testing.T) {
	m := &WebSocketMuxer{}
	applyMuxerOptions(m,
		WithStreamRecvQueueSize(9),
		WithStreamRecvOfferTimeout(5*time.Millisecond),
		WithStreamRecvSaturationThreshold(4),
	)

	assert.Equal(t, 9, m.streamRecvQueueSize)
	assert.Equal(t, 5*time.Millisecond, m.streamRecvOfferTimeout)
	assert.Equal(t, int64(4), m.streamRecvSaturationThreshold)
}

func TestValidateInboundMessageRejectsOversizedPayload(t *testing.T) {
	m := &WebSocketMuxer{maxMessagePayloadBytes: 4}

	err := m.validateInboundMessage(&MuxerMsg{Payload: []byte("12345")})

	require.ErrorIs(t, err, ErrPayloadTooLarge)
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
	m.writeQueueOfferTimeout = 5 * time.Millisecond

	require.NoError(t, m.sendData(uuid.New(), uuid.New(), []byte("seed"), nil))
	require.NoError(t, m.sendPing())

	assert.Equal(t, int64(1), m.WriteQueueBlocks())
	assert.Equal(t, int64(1), m.WriteQueueFallbacks())
	assert.Equal(t, int64(1), m.WriteQueueDepth())
}

func TestShouldBlockBrieflyAndEnqueueWhenQueueDrainsBeforeTimeout(t *testing.T) {
	m := newQueueTestMuxer(1)
	m.writeQueueOfferTimeout = 75 * time.Millisecond

	releaseSend := make(chan struct{})
	sendCalls := atomic.Int32{}
	m.sendJSON = func(_ *websocket.Conn, _ interface{}) error {
		if sendCalls.Add(1) == 1 {
			<-releaseSend
		}
		return nil
	}

	require.NoError(t, m.sendData(uuid.New(), uuid.New(), []byte("first"), nil))

	errCh := make(chan error, 1)
	durationCh := make(chan time.Duration, 1)
	go func() {
		started := time.Now()
		errCh <- m.sendData(uuid.New(), uuid.New(), []byte("second"), nil)
		durationCh <- time.Since(started)
	}()

	time.Sleep(20 * time.Millisecond)
	go m.writePump()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for blocked enqueue to complete")
	}

	blockedFor := <-durationCh
	assert.GreaterOrEqual(t, blockedFor, 20*time.Millisecond)
	assert.Equal(t, int64(1), m.WriteQueueBlocks())
	assert.Equal(t, int64(0), m.WriteQueueFallbacks())
	assert.Equal(t, int64(1), m.WriteQueueDepth())

	close(releaseSend)
	m.shutdown(nil)
	select {
	case <-m.writerDone:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for writer to stop")
	}
}

func TestShouldFallbackAfterBoundedQueueWaitExpires(t *testing.T) {
	m := newQueueTestMuxer(1)
	m.writeQueueOfferTimeout = 25 * time.Millisecond

	require.NoError(t, m.sendData(uuid.New(), uuid.New(), []byte("first"), nil))

	started := time.Now()
	require.NoError(t, m.sendData(uuid.New(), uuid.New(), []byte("second"), nil))
	blockedFor := time.Since(started)

	assert.GreaterOrEqual(t, blockedFor, 20*time.Millisecond)
	assert.Equal(t, int64(1), m.WriteQueueBlocks())
	assert.Equal(t, int64(1), m.WriteQueueFallbacks())
	assert.Equal(t, int64(1), m.WriteQueueDepth())
}

func TestShouldSendAccessDeniedError(t *testing.T) {
	// Arrange
	m := &WebSocketMuxer{
		Context:    context.Background(),
		channels:   make(map[uuid.UUID]*MuxerBidiStream),
		tombstones: make(map[uuid.UUID]error),
		done:       make(chan struct{}),
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
	assert.Equal(t, controlMsgSentinel+"error", em.Type)
	assert.Equal(t, "access denied", em.Err)
}

func TestShouldRouteErrorControlMessagesToExistingStream(t *testing.T) {
	m := &WebSocketMuxer{
		Context:    context.Background(),
		name:       "client",
		channels:   make(map[uuid.UUID]*MuxerBidiStream),
		tombstones: make(map[uuid.UUID]error),
		done:       make(chan struct{}),
	}

	storeID := uuid.New()
	channelID := uuid.New()
	bidi := m.register(context.Background(), storeID, channelID)

	m.processMessage(&MuxerMsg{
		ControlType: ControlTypeError,
		StoreID:     storeID,
		ChannelID:   channelID,
		Payload:     []byte(`{"type":"mux::error","err":"access denied"}`),
	})

	var out string
	assert.EqualError(t, bidi.Decode(&out), "remote error: access denied")
	assert.Equal(t, int64(0), atomic.LoadInt64(&m.activeStreams))
	m.channelsMu.RLock()
	_, exists := m.channels[channelID]
	m.channelsMu.RUnlock()
	assert.False(t, exists, "expected channel to be removed after remote error")
}

func TestShouldReleaseStreamOnPeerCloseWithoutDroppingBufferedMessages(t *testing.T) {
	m := &WebSocketMuxer{
		Context:    context.Background(),
		name:       "client",
		channels:   make(map[uuid.UUID]*MuxerBidiStream),
		tombstones: make(map[uuid.UUID]error),
		done:       make(chan struct{}),
		session:    &muxerSession{allowAll: true},
	}

	storeID := uuid.New()
	channelID := uuid.New()
	bidi := m.register(context.Background(), storeID, channelID)
	require.Equal(t, int64(1), atomic.LoadInt64(&m.activeStreams))

	m.processMessage(&MuxerMsg{
		ControlType: ControlTypeData,
		StoreID:     storeID,
		ChannelID:   channelID,
		Payload:     []byte(`"ok"`),
	})
	m.processMessage(&MuxerMsg{
		ControlType: ControlTypeData,
		StoreID:     storeID,
		ChannelID:   channelID,
		Payload:     []byte(`{"type":"mux::close"}`),
	})

	m.channelsMu.RLock()
	_, exists := m.channels[channelID]
	m.channelsMu.RUnlock()
	assert.False(t, exists, "expected channel to be removed after peer close")
	assert.Equal(t, int64(0), atomic.LoadInt64(&m.activeStreams))

	var value string
	require.NoError(t, bidi.Decode(&value))
	assert.Equal(t, "ok", value)
	assert.Equal(t, io.EOF, bidi.Decode(&value))
}

func TestDeliverToStreamClosesAfterSustainedBackpressure(t *testing.T) {
	sent := make(chan MuxerMsg, 4)
	m := &WebSocketMuxer{
		Context:                       context.Background(),
		name:                          "client",
		channels:                      make(map[uuid.UUID]*MuxerBidiStream),
		tombstones:                    make(map[uuid.UUID]error),
		done:                          make(chan struct{}),
		streamRecvOfferTimeout:        5 * time.Millisecond,
		streamRecvSaturationThreshold: 2,
		sendJSON: func(_ *websocket.Conn, v interface{}) error {
			if msg, ok := v.(*MuxerMsg); ok {
				sent <- *msg
			}
			return nil
		},
	}

	storeID := uuid.New()
	slowID := uuid.New()
	fastID := uuid.New()
	slow := m.register(context.Background(), storeID, slowID)
	fast := m.register(context.Background(), storeID, fastID)

	for i := 0; i < cap(slow.recvChan); i++ {
		require.True(t, slow.Offer([]byte(`"blocked"`)))
	}

	m.deliverToStream(slow, context.Background(), &MuxerMsg{
		ControlType: ControlTypeData,
		StoreID:     storeID,
		ChannelID:   slowID,
		Payload:     []byte(`"overflow"`),
	})

	require.True(t, slow.IsClosed())
	assert.ErrorIs(t, slow.Decode(new(string)), ErrStreamOverloaded)
	assert.GreaterOrEqual(t, m.StreamRecvBlocks(), int64(2))
	assert.GreaterOrEqual(t, m.StreamRecvTimeouts(), int64(2))
	assert.Equal(t, int64(1), m.StreamRecvOverloads())

	m.deliverToStream(fast, context.Background(), &MuxerMsg{
		ControlType: ControlTypeData,
		StoreID:     storeID,
		ChannelID:   fastID,
		Payload:     []byte(`"ok"`),
	})

	var fastValue string
	require.NoError(t, fast.Decode(&fastValue))
	assert.Equal(t, "ok", fastValue)

	m.processMessage(&MuxerMsg{ControlType: ControlTypePing})

	foundError := false
	foundPong := false
	require.Eventually(t, func() bool {
		for len(sent) > 0 {
			msg := <-sent
			if msg.ControlType == ControlTypeError && msg.ChannelID == slowID {
				foundError = true
			}
			if msg.ControlType == ControlTypePong {
				foundPong = true
			}
		}
		return foundError && foundPong
	}, time.Second, 10*time.Millisecond)
}

func TestDeliverToStreamWaitsForTemporaryBackpressure(t *testing.T) {
	m := &WebSocketMuxer{
		Context:                       context.Background(),
		name:                          "client",
		channels:                      make(map[uuid.UUID]*MuxerBidiStream),
		tombstones:                    make(map[uuid.UUID]error),
		done:                          make(chan struct{}),
		streamRecvOfferTimeout:        5 * time.Millisecond,
		streamRecvSaturationThreshold: 2,
		sendJSON:                      func(_ *websocket.Conn, _ interface{}) error { return nil },
	}

	storeID := uuid.New()
	slowID := uuid.New()
	fastID := uuid.New()
	slow := m.register(context.Background(), storeID, slowID)
	fast := m.register(context.Background(), storeID, fastID)

	slow.recvChan = make(chan any, 1)
	require.True(t, slow.Offer([]byte(`"blocked"`)))

	done := make(chan struct{})
	go func() {
		m.deliverToStream(slow, context.Background(), &MuxerMsg{
			ControlType: ControlTypeData,
			StoreID:     storeID,
			ChannelID:   slowID,
			Payload:     []byte(`"recovered"`),
		})
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("deliverToStream should wait for headroom before returning")
	default:
	}

	<-slow.recvChan

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("deliverToStream did not resume after temporary backpressure")
	}

	assert.False(t, slow.IsClosed())
	assert.GreaterOrEqual(t, m.StreamRecvTimeouts(), int64(1))
	var slowValue string
	require.NoError(t, slow.Decode(&slowValue))
	assert.Equal(t, "recovered", slowValue)

	m.deliverToStream(fast, context.Background(), &MuxerMsg{
		ControlType: ControlTypeData,
		StoreID:     storeID,
		ChannelID:   fastID,
		Payload:     []byte(`"ok"`),
	})
	var fastValue string
	require.NoError(t, fast.Decode(&fastValue))
	assert.Equal(t, "ok", fastValue)
}

func TestGetOrCreateStreamIgnoresTombstonedChannel(t *testing.T) {
	manager := &fakeNodeManager{}
	channelID := uuid.New()
	m := &WebSocketMuxer{
		Context:     context.Background(),
		name:        "server",
		channels:    make(map[uuid.UUID]*MuxerBidiStream),
		tombstones:  map[uuid.UUID]error{channelID: ErrStreamOverloaded},
		done:        make(chan struct{}),
		nodeManager: manager,
		session:     &muxerSession{allowAll: true},
	}

	_, err := m.getOrCreateStream(context.Background(), &MuxerMsg{
		ControlType: ControlTypeData,
		StoreID:     uuid.New(),
		ChannelID:   channelID,
		Payload:     []byte(`"late"`),
	})

	assert.ErrorIs(t, err, ErrStreamTombstoned)
	assert.Equal(t, int32(0), manager.getCalls.Load())
}

func TestSuccessfulWritesDoNotRefreshHeartbeatTimeout(t *testing.T) {
	m := &WebSocketMuxer{
		Context:     context.Background(),
		done:        make(chan struct{}),
		pongTimeout: 1,
		sendJSON:    func(_ *websocket.Conn, _ interface{}) error { return nil },
	}
	atomic.StoreInt64(&m.lastPongUnix, 0)

	require.NoError(t, m.sendJSONWithLock(&MuxerMsg{ControlType: ControlTypePing}, false))
	assert.True(t, m.checkHeartbeatTimeout())
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
