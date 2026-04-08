// Package wskit implements WebSocket-based multiplexing used by streamkit.
// It provides a muxer that multiplexes logical bidirectional streams over a
// single WebSocket connection.
package wskit

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"sync"
	"time"

	"sync/atomic"

	"github.com/fgrzl/timestamp"

	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/server"
	"github.com/fgrzl/streamkit/pkg/telemetry"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/net/websocket"
)

type ControlType string

const (
	ControlTypeData  ControlType = "data"
	ControlTypePing  ControlType = "ping"
	ControlTypePong  ControlType = "pong"
	ControlTypeClose ControlType = "close"
	ControlTypeError ControlType = "error"
)

// MuxerMsg represents a framed message sent over the multiplexed WebSocket.
// Each message is scoped to a specific logical channel by ChannelID.
// MuxerMsg is the wire representation for framed messages exchanged over the
// multiplexed websocket. The ControlType field selects the frame kind; the
// StoreID and ChannelID scope payloads to logical streams.
// TraceContext carries W3C trace context (traceparent, baggage) for distributed tracing.
type MuxerMsg struct {
	// ControlType uses type tag (ignored by encoding/json) so the field keeps
	// serializing as "ControlType" for wire compatibility; do not add json tag.
	ControlType  ControlType       `type:"control_type"`
	StoreID      uuid.UUID         `json:"store_id"`
	ChannelID    uuid.UUID         `json:"channel_id"`
	Payload      []byte            `json:"payload"`
	TraceContext map[string]string `json:"trace_context,omitempty"`
}

// WebSocketMuxer multiplexes multiple logical bidirectional streams over a single WebSocket connection.
// Each logical stream is identified by a ChannelID.
// WebSocketMuxer multiplexes logical bidirectional streams over a single
// websocket.Conn. It tracks per-stream channels and heartbeat state.
type WebSocketMuxer struct {
	Context     context.Context
	session     MuxerSession
	name        string
	conn        *websocket.Conn
	channels    map[uuid.UUID]*MuxerBidiStream
	tombstones  map[uuid.UUID]error
	channelsMu  sync.RWMutex
	writeMu     sync.Mutex
	done        chan struct{}
	nodeManager server.NodeManager
	closeOnce   sync.Once

	// outbound write pump
	writeQueue             chan *MuxerMsg
	writeQueueSize         int
	writeQueueOfferTimeout time.Duration
	queueMetrics           *telemetry.WSKitQueueMetrics
	msgPool                sync.Pool
	bufPool                sync.Pool
	writerDone             chan struct{}

	// Heartbeat configuration (seconds)
	pingInterval  int64
	pongTimeout   int64
	lastPongUnix  int64 // atomic; tracks the most recent inbound activity/pong
	heartbeatStop chan struct{}
	cancelFunc    context.CancelFunc
	pingJitter    int64
	maxStreams    int64

	// frame/message size guardrails
	maxFramePayloadBytes   int
	maxMessagePayloadBytes int

	// runtime counters
	pingsSent                    int64
	pingsReceived                int64
	pongsReceived                int64
	missedPongs                  int64
	writeErrors                  int64
	activeStreams                int64 // number of streams currently registered (in-flight)
	writeQueueBlocks             int64
	writeQueueDepth              int64
	writeQueueFallbacks          int64
	writeQueueSaturationStreak   int64
	writeQueueSaturationWarnings int64
	logger                       *slog.Logger
	rng                          *rand.Rand
	// JSON send/receive hooks (set to websocket.JSON.Send/Receive by default).
	// Tests may override these to simulate network behavior.
	sendJSON func(conn *websocket.Conn, v interface{}) error
	recvJSON func(conn *websocket.Conn, v interface{}) error
}

// sentinel errors
var (
	ErrMuxerClosed      = errors.New("muxer closed")
	ErrHeartbeatTimeout = errors.New("heartbeat timeout")
	ErrStreamOverloaded = errors.New("stream receive buffer overloaded")
	ErrStreamTombstoned = errors.New("stream closed locally")
	ErrTooManyStreams   = errors.New("too many active streams")
	ErrPayloadTooLarge  = errors.New("muxer payload too large")
)

const (
	writeQueueSaturationWarnThreshold int64 = 32
	writeQueueSaturationWarnEvery     int64 = 256
	defaultWriteQueueOfferTimeout           = 5 * time.Millisecond
	defaultMaxLogicalStreams                = 0
	defaultMaxFramePayloadBytes             = 8 << 20
	defaultMaxMessagePayloadBytes           = 6 << 20
	defaultStreamRecvQueueSize              = 512
	defaultStreamRecvOfferTimeout           = 100 * time.Millisecond
)

// NewClientWebSocketMuxer will spawn a read loop as a go routine and returns the *WebSocketMuxer
// NewClientWebSocketMuxer creates a client-side muxer, starts its read loop
// and heartbeat goroutines, and returns the muxer instance.
func NewClientWebSocketMuxer(ctx context.Context, session MuxerSession, conn *websocket.Conn) *WebSocketMuxer {
	cctx, cancel := context.WithCancel(ctx)
	m := &WebSocketMuxer{
		Context:                ctx,
		session:                session,
		name:                   "client",
		conn:                   conn,
		channels:               make(map[uuid.UUID]*MuxerBidiStream),
		tombstones:             make(map[uuid.UUID]error),
		done:                   make(chan struct{}),
		pingInterval:           30,
		pongTimeout:            90,
		pingJitter:             5,
		maxStreams:             defaultMaxLogicalStreams,
		maxFramePayloadBytes:   defaultMaxFramePayloadBytes,
		maxMessagePayloadBytes: defaultMaxMessagePayloadBytes,
		lastPongUnix:           timestamp.GetTimestamp(),
		heartbeatStop:          make(chan struct{}),
		cancelFunc:             cancel,
		writeQueueOfferTimeout: defaultWriteQueueOfferTimeout,
		queueMetrics:           telemetry.NewWSKitQueueMetrics(),
	}

	// write pump defaults
	m.writeQueueSize = 1024
	m.writeQueue = make(chan *MuxerMsg, m.writeQueueSize)
	m.msgPool = sync.Pool{New: func() any { return &MuxerMsg{} }}
	m.bufPool = sync.Pool{New: func() any { return make([]byte, 0, 1024) }}
	m.writerDone = make(chan struct{})
	m.logger = slog.With(slog.String("muxer", m.name))
	// per-muxer logger with common fields
	m.Context = cctx
	// per-muxer RNG for jittered heartbeat
	m.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	// initialize lastPongUnix atomically
	atomic.StoreInt64(&m.lastPongUnix, timestamp.GetTimestamp())
	// default send/recv use websocket.JSON
	m.sendJSON = func(conn *websocket.Conn, v interface{}) error { return websocket.JSON.Send(conn, v) }
	m.recvJSON = func(conn *websocket.Conn, v interface{}) error { return websocket.JSON.Receive(conn, v) }
	m.applyConnectionLimits()
	go m.readLoop()
	go m.heartbeat()
	go m.writePump()
	return m
}

// NewServerWebSocketMuxer will start a blocking read loop to keep the websocket connection open
// NewServerWebSocketMuxer constructs a server-side muxer, starts the
// heartbeat goroutine and blocks in readLoop until the connection closes.
func NewServerWebSocketMuxer(ctx context.Context, session MuxerSession, nodeManager server.NodeManager, conn *websocket.Conn) {
	cctx, cancel := context.WithCancel(ctx)
	m := &WebSocketMuxer{
		Context:                cctx,
		session:                session,
		name:                   "server",
		conn:                   conn,
		nodeManager:            nodeManager,
		channels:               make(map[uuid.UUID]*MuxerBidiStream),
		tombstones:             make(map[uuid.UUID]error),
		done:                   make(chan struct{}),
		pingInterval:           30,
		pongTimeout:            90,
		pingJitter:             5,
		maxStreams:             defaultMaxLogicalStreams,
		maxFramePayloadBytes:   defaultMaxFramePayloadBytes,
		maxMessagePayloadBytes: defaultMaxMessagePayloadBytes,
		lastPongUnix:           timestamp.GetTimestamp(),
		heartbeatStop:          make(chan struct{}),
		cancelFunc:             cancel,
		writeQueueOfferTimeout: defaultWriteQueueOfferTimeout,
		queueMetrics:           telemetry.NewWSKitQueueMetrics(),
	}
	m.Context = cctx
	m.logger = slog.With(slog.String("muxer", m.name))
	m.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	atomic.StoreInt64(&m.lastPongUnix, timestamp.GetTimestamp())
	m.sendJSON = func(conn *websocket.Conn, v interface{}) error { return websocket.JSON.Send(conn, v) }
	m.recvJSON = func(conn *websocket.Conn, v interface{}) error { return websocket.JSON.Receive(conn, v) }
	m.applyConnectionLimits()
	// Initialize write pump infrastructure like the client muxer so server
	// behavior is symmetric and safe for concurrent use.
	m.writeQueueSize = 1024
	m.writeQueue = make(chan *MuxerMsg, m.writeQueueSize)
	m.msgPool = sync.Pool{New: func() any { return &MuxerMsg{} }}
	m.bufPool = sync.Pool{New: func() any { return make([]byte, 0, 1024) }}
	m.writerDone = make(chan struct{})

	// per-muxer logger and RNG already assigned above

	go m.heartbeat()
	go m.writePump()
	m.readLoop()
}

// Ping sends a control ping and returns true if:
// 1. The muxer is not closed
// 2. The ping can be sent/queued successfully
// 3. We've received recent activity within the pong timeout window
// This provides a more accurate health check than just checking if send works.
func (m *WebSocketMuxer) Ping() bool {
	// Fast fail if already closed
	select {
	case <-m.done:
		return false
	default:
	}

	// Check if we've had recent activity - if lastPongUnix is too old,
	// the connection is likely dead even if sendPing would succeed
	ts := timestamp.GetTimestamp()
	last := atomic.LoadInt64(&m.lastPongUnix)
	pongTimeoutMs := m.pongTimeout * 1000
	idleMs := ts - last
	if idleMs > pongTimeoutMs {
		return false
	}

	if err := m.sendPing(); err == nil {
		return true
	}
	return false
}

func (m *WebSocketMuxer) applyConnectionLimits() {
	if m == nil || m.conn == nil || m.maxFramePayloadBytes <= 0 {
		return
	}
	m.conn.MaxPayloadBytes = m.maxFramePayloadBytes
}

// Serve blocks until the WebSocket connection is closed or an error occurs.
func (m *WebSocketMuxer) Serve() {
	<-m.done
}

// Register creates and tracks a new stream for the given ChannelID.
// If a stream with this ID already exists, it is overwritten.
// Register creates or overwrites a logical bidi stream for the provided
// channelID and returns an api.BidiStream that can be used to send and receive
// payloads on that logical stream.
func (m *WebSocketMuxer) Register(storeID, channelID uuid.UUID) api.BidiStream {
	return m.RegisterWithContext(context.Background(), storeID, channelID)
}

// RegisterWithContext is like Register but uses ctx for trace context propagation
// when sending data frames. Used by the provider to correlate client and server spans.
func (m *WebSocketMuxer) RegisterWithContext(ctx context.Context, storeID, channelID uuid.UUID) api.BidiStream {
	return m.register(ctx, storeID, channelID)
}

// internal registration logic (safe for reuse)
func (m *WebSocketMuxer) register(ctx context.Context, storeID, channelID uuid.UUID) *MuxerBidiStream {
	// Fast-fail if the muxer is already shut down. Without this check,
	// Register on a closed muxer creates an orphaned stream in the channels
	// map that will never be cleaned up by shutdown (which already ran).
	// The stream's sendFn still fast-fails via sendData, but we avoid the
	// unnecessary allocation and map entry.
	select {
	case <-m.done:
		// Return a stream whose sendFn immediately returns ErrMuxerClosed.
		// This keeps the interface contract intact — caller gets a stream
		// that fails on first Encode rather than requiring a nil check.
		bidi := NewMuxerBidiStream(
			func([]byte) error { return ErrMuxerClosed },
			func() {},
		)
		bidi.SetChannelID(channelID)
		return bidi
	default:
	}
	if m.maxStreams > 0 && atomic.LoadInt64(&m.activeStreams) >= m.maxStreams {
		bidi := NewMuxerBidiStream(
			func([]byte) error { return ErrTooManyStreams },
			func() {},
		)
		bidi.SetChannelID(channelID)
		return bidi
	}

	sendFn := func(payload []byte) error {
		// Only client-originated data frames should inherit the request trace.
		// Server-side streams can live for hours (subscriptions/streaming responses),
		// so reusing the registration-time trace context would grow one trace forever.
		if m.name == "client" {
			return m.sendDataWithTrace(ctx, storeID, channelID, payload)
		}
		return m.sendData(storeID, channelID, payload, nil)
	}

	cleanup := func() {
		atomic.AddInt64(&m.activeStreams, -1)
		m.channelsMu.Lock()
		defer m.channelsMu.Unlock()
		delete(m.channels, channelID)
	}

	bidi := NewMuxerBidiStream(sendFn, cleanup)
	bidi.SetChannelID(channelID)

	m.channelsMu.Lock()
	delete(m.tombstones, channelID)
	m.channels[channelID] = bidi
	m.channelsMu.Unlock()

	atomic.AddInt64(&m.activeStreams, 1)

	return bidi
}

func (m *WebSocketMuxer) tombstoneStream(channelID uuid.UUID, reason error) {
	if channelID == uuid.Nil {
		return
	}

	m.channelsMu.Lock()
	if m.tombstones == nil {
		m.tombstones = make(map[uuid.UUID]error)
	}
	if existing, exists := m.tombstones[channelID]; !exists || existing == nil || reason != nil {
		m.tombstones[channelID] = reason
	}
	bidi := m.channels[channelID]
	m.channelsMu.Unlock()

	if bidi != nil {
		bidi.CloseLocal(reason)
	}
}

// sendDataWithTrace injects trace context from ctx into the message and sends a data frame.
func (m *WebSocketMuxer) sendDataWithTrace(ctx context.Context, storeID, channelID uuid.UUID, payload []byte) error {
	var traceContext map[string]string
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		traceContext = make(map[string]string)
		otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(traceContext))
	}
	return m.sendData(storeID, channelID, payload, traceContext)
}

// sendData sends a data control frame for the given store/channel and handles
// error reporting and shutdown logic. traceContext is optional and used for distributed tracing.
func (m *WebSocketMuxer) sendData(storeID, channelID uuid.UUID, payload []byte, traceContext map[string]string) error {
	// fast-fail if muxer is closed
	select {
	case <-m.done:
		return ErrMuxerClosed
	default:
	}
	if m.maxMessagePayloadBytes > 0 && len(payload) > m.maxMessagePayloadBytes {
		return ErrPayloadTooLarge
	}

	// prepare pooled message (if available)
	pooled := m.msgPool.Get()
	if pooled == nil || m.writeQueue == nil {
		// synchronous fallback when pool or queue not initialized - shutdown on error
		msg := &MuxerMsg{ControlType: ControlTypeData, StoreID: storeID, ChannelID: channelID, Payload: payload, TraceContext: traceContext}
		return m.sendJSONWithLock(msg, true)
	}

	msg := pooled.(*MuxerMsg)
	msg.ControlType = ControlTypeData
	msg.StoreID = storeID
	msg.ChannelID = channelID
	msg.TraceContext = traceContext
	if len(payload) > 0 {
		// try to reuse buffer from pool when possible
		bp := m.bufPool.Get()
		if bp != nil {
			if buf, ok := bp.([]byte); ok && cap(buf) >= len(payload) {
				b := buf[:len(payload)]
				copy(b, payload)
				msg.Payload = b
			} else {
				nb := make([]byte, len(payload))
				copy(nb, payload)
				msg.Payload = nb
			}
		} else {
			nb := make([]byte, len(payload))
			copy(nb, payload)
			msg.Payload = nb
		}
	} else {
		msg.Payload = nil
	}

	return m.enqueueWriteMessage(msg, func() error {
		return m.sendJSONWithLock(&MuxerMsg{ControlType: ControlTypeData, StoreID: storeID, ChannelID: channelID, Payload: payload, TraceContext: traceContext}, true)
	})
}

func (m *WebSocketMuxer) metricsContext() context.Context {
	if m.Context != nil {
		return m.Context
	}
	return context.Background()
}

func (m *WebSocketMuxer) writeQueueCapacity() int {
	if m.writeQueueSize > 0 {
		return m.writeQueueSize
	}
	if m.writeQueue != nil {
		return cap(m.writeQueue)
	}
	return 0
}

func (m *WebSocketMuxer) writeQueueWaitTimeout() time.Duration {
	if m.writeQueueOfferTimeout > 0 {
		return m.writeQueueOfferTimeout
	}
	return defaultWriteQueueOfferTimeout
}

func (m *WebSocketMuxer) snapshotWriteQueueDepth() int64 {
	depth := int64(0)
	if m.writeQueue != nil {
		depth = int64(len(m.writeQueue))
	}
	atomic.StoreInt64(&m.writeQueueDepth, depth)
	if m.queueMetrics != nil {
		m.queueMetrics.RecordWriteQueueDepth(m.metricsContext(), m.name, depth)
	}
	return depth
}

func (m *WebSocketMuxer) recordWriteQueueEnqueue() {
	atomic.StoreInt64(&m.writeQueueSaturationStreak, 0)
	m.snapshotWriteQueueDepth()
}

func (m *WebSocketMuxer) recordWriteQueueBlocked(duration time.Duration) {
	atomic.AddInt64(&m.writeQueueBlocks, 1)
	if m.queueMetrics != nil {
		m.queueMetrics.RecordWriteQueueBlocked(m.metricsContext(), m.name, duration)
	}
}

func (m *WebSocketMuxer) recordWriteQueueDrain() {
	depth := m.snapshotWriteQueueDepth()
	capacity := m.writeQueueCapacity()
	if capacity <= 0 || depth < int64(capacity) {
		atomic.StoreInt64(&m.writeQueueSaturationStreak, 0)
	}
}

func (m *WebSocketMuxer) recordWriteQueueFallback() {
	depth := m.snapshotWriteQueueDepth()
	fallbacks := atomic.AddInt64(&m.writeQueueFallbacks, 1)
	streak := atomic.AddInt64(&m.writeQueueSaturationStreak, 1)
	if m.queueMetrics != nil {
		m.queueMetrics.RecordWriteQueueFallback(m.metricsContext(), m.name)
	}
	if streak == writeQueueSaturationWarnThreshold || (streak > writeQueueSaturationWarnThreshold && streak%writeQueueSaturationWarnEvery == 0) {
		warnings := atomic.AddInt64(&m.writeQueueSaturationWarnings, 1)
		if m.queueMetrics != nil {
			m.queueMetrics.RecordWriteQueueSaturation(m.metricsContext(), m.name)
		}
		slog.WarnContext(m.metricsContext(), "muxer: write queue saturated; falling back to synchronous send",
			m.logFields(m.metricsContext(),
				slog.Int64("queue_depth", depth),
				slog.Int("queue_capacity", m.writeQueueCapacity()),
				slog.Int64("queue_fallbacks", fallbacks),
				slog.Int64("saturation_streak", streak),
				slog.Int64("saturation_warnings", warnings))...)
	}
}

func (m *WebSocketMuxer) enqueueWriteMessage(msg *MuxerMsg, fallback func() error) error {
	select {
	case m.writeQueue <- msg:
		m.recordWriteQueueEnqueue()
		return nil
	default:
	}

	blockedAt := time.Now()
	timer := time.NewTimer(m.writeQueueWaitTimeout())
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	select {
	case <-m.done:
		m.releaseMsg(msg)
		return ErrMuxerClosed
	case m.writeQueue <- msg:
		m.recordWriteQueueBlocked(time.Since(blockedAt))
		m.recordWriteQueueEnqueue()
		return nil
	case <-timer.C:
		m.recordWriteQueueBlocked(time.Since(blockedAt))
		m.releaseMsg(msg)
		m.recordWriteQueueFallback()
		return fallback()
	}
}

// readLoop continuously receives messages from the WebSocket,
// routes them to the appropriate stream, and auto-registers new streams.
func (m *WebSocketMuxer) readLoop() {
	for {
		var msg MuxerMsg
		var err error

		err = m.recvJSON(m.conn, &msg)

		if err != nil {
			// Delegate receive-error handling (logs + shutdown) to helper for clarity.
			m.handleReceiveError(err)
			return
		}
		if err := m.validateInboundMessage(&msg); err != nil {
			m.handleReceiveError(err)
			return
		}

		// refresh activity time on any received message
		atomic.StoreInt64(&m.lastPongUnix, timestamp.GetTimestamp())

		// Delegate control-type handling to a helper to keep the loop body small.
		m.processMessage(&msg)
	}
}

// handleReceiveError centralizes logging and shutdown behavior when websocket.Receive fails.
func (m *WebSocketMuxer) handleReceiveError(err error) {
	// Normal disconnect: clean close, RST, or abrupt hangup — not an application fault.
	if benignDisconnect(err) {
		slog.DebugContext(m.Context, "muxer: websocket disconnected",
			slog.String("muxer", m.name),
			slog.String("error_type", classifyTransportError(err)),
			slog.String("detail", err.Error()))
		m.shutdown(ErrMuxerClosed)
		return
	}
	// Unwrap net errors for richer logs
	var (
		netErr net.Error
		opErr  *net.OpError
	)
	fields := []any{
		slog.String("muxer", m.name),
		slog.String("error_type", classifyTransportError(err)),
	}
	if errors.As(err, &netErr) {
		fields = append(fields, slog.String("net_err", netErr.Error()), slog.Bool("temporary", netErr.Temporary()))
	}
	if errors.As(err, &opErr) && opErr != nil {
		if opErr.Op != "" {
			fields = append(fields, slog.String("op", opErr.Op))
		}
		if opErr.Net != "" {
			fields = append(fields, slog.String("net", opErr.Net))
		}
		if opErr.Addr != nil {
			fields = append(fields, slog.String("addr", opErr.Addr.String()))
		}
		if opErr.Err != nil {
			fields = append(fields, slog.String("op_err", opErr.Err.Error()))
		}
	}
	fields = append(fields, slog.String("err", err.Error()))
	slog.WarnContext(m.Context, "muxer: websocket receive error", fields...)
	m.shutdown(err)
}

func (m *WebSocketMuxer) logFields(ctx context.Context, extras ...any) []any {
	fields := []any{slog.String("muxer", m.name)}
	if channelID, ok := server.ChannelIDFromContext(ctx); ok {
		fields = append(fields, slog.String("channel_id", channelID.String()))
	}
	fields = append(fields, extras...)
	return fields
}

func (m *WebSocketMuxer) msgLogFields(msg *MuxerMsg, extras ...any) []any {
	fields := []any{slog.String("muxer", m.name)}
	if msg != nil {
		if msg.StoreID != uuid.Nil {
			fields = append(fields, slog.String("store_id", msg.StoreID.String()))
		}
		fields = append(fields, slog.String("channel_id", msg.ChannelID.String()))
	}
	fields = append(fields, extras...)
	return fields
}

// processMessage handles a single decoded MuxerMsg. It contains the large
// control-type switch extracted from readLoop so the loop itself stays concise.
// For data messages, trace context is extracted from the frame and attached to context.
func (m *WebSocketMuxer) processMessage(msg *MuxerMsg) {
	ctx := server.WithChannelID(m.Context, msg.ChannelID)
	if len(msg.TraceContext) > 0 {
		ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(msg.TraceContext))
	}
	switch msg.ControlType {
	case ControlTypePing:
		m.handlePing(ctx)
	case ControlTypePong:
		m.handlePong(ctx)
	case ControlTypeClose:
		m.handleClose(ctx)
		return
	case ControlTypeError:
		m.handleErrorMessage(ctx, msg)
	case ControlTypeData:
		m.handleDataMessage(ctx, msg)
	default:
		slog.WarnContext(ctx, "muxer: unrecognized control type",
			m.logFields(ctx, slog.String("type", string(msg.ControlType)))...)
	}
}

func (m *WebSocketMuxer) validateInboundMessage(msg *MuxerMsg) error {
	if msg == nil {
		return nil
	}
	if m.maxMessagePayloadBytes > 0 && len(msg.Payload) > m.maxMessagePayloadBytes {
		return ErrPayloadTooLarge
	}
	return nil
}

func (m *WebSocketMuxer) handlePing(_ context.Context) {
	atomic.AddInt64(&m.pingsReceived, 1)
	if err := m.sendControl(ControlTypePong, uuid.Nil, uuid.Nil, nil); err != nil {
		slog.WarnContext(m.Context, "muxer: pong send failed",
			m.logFields(m.Context,
				slog.String("error_type", classifyTransportError(err)),
				"err", err)...)
	}
	atomic.StoreInt64(&m.lastPongUnix, timestamp.GetTimestamp())
}

func (m *WebSocketMuxer) handlePong(_ context.Context) {
	ts := timestamp.GetTimestamp()
	atomic.AddInt64(&m.pongsReceived, 1)
	atomic.StoreInt64(&m.lastPongUnix, ts)
}

func (m *WebSocketMuxer) handleClose(ctx context.Context) {
	slog.DebugContext(ctx, "muxer: received close", slog.String("muxer", m.name))
	m.shutdown(ErrMuxerClosed)
}

func (m *WebSocketMuxer) handleErrorMessage(ctx context.Context, msg *MuxerMsg) {
	if msg != nil && msg.ChannelID != uuid.Nil {
		if m.handlePeerStreamControl(ctx, msg) {
			return
		}
		m.channelsMu.RLock()
		bidi, exists := m.channels[msg.ChannelID]
		_, tombstoned := m.tombstones[msg.ChannelID]
		m.channelsMu.RUnlock()
		if exists {
			m.deliverToStream(bidi, ctx, msg)
			return
		}
		if tombstoned {
			slog.DebugContext(ctx, "muxer: ignoring error control for closed channel", m.msgLogFields(msg)...)
			return
		}
	}

	slog.WarnContext(ctx, "muxer: received error control message",
		m.msgLogFields(msg,
			slog.String("payload", string(msg.Payload)))...,
	)
}

func (m *WebSocketMuxer) handleDataMessage(ctx context.Context, msg *MuxerMsg) {
	if !m.canAccessOrSendError(ctx, msg.StoreID, msg.ChannelID) {
		return
	}
	if m.handlePeerStreamControl(ctx, msg) {
		return
	}

	bidi, err := m.getOrCreateStream(ctx, msg)
	if err != nil {
		// getOrCreateStream already logged and sent an error control if appropriate
		return
	}

	m.deliverToStream(bidi, ctx, msg)
}

func (m *WebSocketMuxer) handlePeerStreamControl(ctx context.Context, msg *MuxerMsg) bool {
	errMsg, ok := parsePeerStreamControl(msg)
	if !ok {
		return false
	}

	m.channelsMu.RLock()
	bidi, exists := m.channels[msg.ChannelID]
	_, tombstoned := m.tombstones[msg.ChannelID]
	m.channelsMu.RUnlock()

	if !exists {
		if tombstoned {
			slog.DebugContext(ctx, "muxer: ignoring peer stream control for closed channel", m.msgLogFields(msg)...)
		} else {
			slog.DebugContext(ctx, "muxer: ignoring peer stream control for unknown channel", m.msgLogFields(msg)...)
		}
		return true
	}

	bidi.CloseRemote(peerStreamControlErr(errMsg))
	return true
}

func parsePeerStreamControl(msg *MuxerMsg) (*ErrorMessage, bool) {
	if msg == nil || len(msg.Payload) == 0 {
		return nil, false
	}

	var errMsg ErrorMessage
	if err := json.Unmarshal(msg.Payload, &errMsg); err != nil {
		return nil, false
	}

	switch errMsg.Type {
	case controlMsgSentinel + "close", controlMsgSentinel + "error":
		return &errMsg, true
	default:
		return nil, false
	}
}

func peerStreamControlErr(errMsg *ErrorMessage) error {
	if errMsg == nil {
		return nil
	}

	switch errMsg.Type {
	case controlMsgSentinel + "close":
		if errMsg.Err == "" {
			return nil
		}
		return errors.New("remote closed stream: " + errMsg.Err)
	case controlMsgSentinel + "error":
		return errors.New("remote error: " + errMsg.Err)
	default:
		return nil
	}
}

// canAccessOrSendError checks store access and sends an error control frame on denial.
func (m *WebSocketMuxer) canAccessOrSendError(ctx context.Context, storeID, channelID uuid.UUID) bool {
	if m.session.CanAccessStore(storeID) {
		return true
	}
	payload, err := json.Marshal(&ErrorMessage{Type: controlMsgSentinel + "error", Err: "access denied"})
	if err != nil {
		slog.WarnContext(ctx, "muxer: marshal access-denied error failed",
			m.logFields(ctx, slog.String("store_id", storeID.String()), "err", err)...)
		return false
	}
	if err := m.sendControl(ControlTypeError, storeID, channelID, payload); err != nil {
		slog.WarnContext(ctx, "muxer: send error control failed",
			m.logFields(ctx,
				slog.String("store_id", storeID.String()),
				slog.String("channel_id", channelID.String()),
				slog.String("error_type", classifyTransportError(err)),
				"err", err)...)
	}
	return false
}

// getOrCreateStream returns a registered bidi stream for the message's channel.
// If the stream doesn't exist, it attempts to use nodeManager to create the
// node and register a new stream. On failure it logs and sends an error control.
func (m *WebSocketMuxer) getOrCreateStream(ctx context.Context, msg *MuxerMsg) (*MuxerBidiStream, error) {
	m.channelsMu.RLock()
	bidi, exists := m.channels[msg.ChannelID]
	_, tombstoned := m.tombstones[msg.ChannelID]
	m.channelsMu.RUnlock()

	if exists {
		return bidi, nil
	}
	if tombstoned {
		slog.DebugContext(ctx, "muxer: ignoring late message for closed channel", m.msgLogFields(msg)...)
		return nil, ErrStreamTombstoned
	}
	if m.maxStreams > 0 && atomic.LoadInt64(&m.activeStreams) >= m.maxStreams {
		err := ErrTooManyStreams
		slog.WarnContext(ctx, "muxer: active stream limit reached",
			m.msgLogFields(msg,
				slog.Int64("active_streams", atomic.LoadInt64(&m.activeStreams)),
				slog.Int64("max_streams", m.maxStreams),
				slog.String("error_type", classifyTransportError(err)),
				"err", err)...)
		m.tombstoneStream(msg.ChannelID, err)
		payload, marshalErr := json.Marshal(&ErrorMessage{Type: controlMsgSentinel + "error", Err: err.Error()})
		if marshalErr != nil {
			slog.WarnContext(ctx, "muxer: marshal stream-limit error failed",
				m.msgLogFields(msg, "err", marshalErr)...)
		} else if sendErr := m.sendControl(ControlTypeError, msg.StoreID, msg.ChannelID, payload); sendErr != nil {
			slog.WarnContext(ctx, "muxer: send stream-limit error failed",
				m.msgLogFields(msg,
					slog.String("error_type", classifyTransportError(sendErr)),
					"err", sendErr)...)
		}
		return nil, err
	}

	if m.nodeManager == nil {
		if m.name == "server" {
			slog.ErrorContext(ctx, "muxer: node manager is nil on server side", m.msgLogFields(msg)...)
			return nil, errors.New("node manager nil")
		}
		return nil, errors.New("node manager nil")
	}

	instance, err := m.nodeManager.GetOrCreate(ctx, msg.StoreID)
	if err != nil {
		slog.ErrorContext(ctx, "muxer: failed to get or create node",
			m.msgLogFields(msg,
				slog.String("err", err.Error()))...)
		payload, marshalErr := json.Marshal(&ErrorMessage{Type: controlMsgSentinel + "error", Err: "store unavailable"})
		if marshalErr != nil {
			slog.WarnContext(ctx, "muxer: marshal store-unavailable error failed",
				m.msgLogFields(msg, "err", marshalErr)...)
		} else if sendErr := m.sendControl(ControlTypeError, msg.StoreID, msg.ChannelID, payload); sendErr != nil {
			slog.WarnContext(ctx, "muxer: send error control failed",
				m.msgLogFields(msg,
					slog.String("error_type", classifyTransportError(sendErr)),
					"err", sendErr)...)
		}
		return nil, err
	}

	bidi = m.register(ctx, msg.StoreID, msg.ChannelID)
	if bidi != nil {
		handleCtx := ctx
		if trace.SpanFromContext(ctx).SpanContext().IsValid() {
			tracer := telemetry.GetTracer()
			reqCtx, span := tracer.Start(ctx, "streamkit.transport.server.request",
				trace.WithAttributes(
					telemetry.WithStoreID(msg.StoreID),
					telemetry.WithChannelID(msg.ChannelID),
					telemetry.WithTransportType("websocket"),
				))
			span.End() // short-lived: stream accepted; avoid long-lived span for Handle()
			handleCtx = reqCtx
			go func() {
				instance.Handle(handleCtx, bidi)
			}()
		} else {
			go func() {
				instance.Handle(handleCtx, bidi)
			}()
		}
		return bidi, nil
	}
	slog.WarnContext(ctx, "muxer: stream registration returned nil bidi", m.msgLogFields(msg)...)
	return nil, errors.New("registration returned nil")
}

// deliverToStream offers the payload to the bidi stream and logs delivery or drop.
func (m *WebSocketMuxer) deliverToStream(bidi *MuxerBidiStream, ctx context.Context, msg *MuxerMsg) {
	if bidi == nil {
		slog.ErrorContext(ctx, "muxer: cannot deliver message, bidi is nil", m.msgLogFields(msg)...)
		return
	}
	if bidi.Offer(msg.Payload) {
		return
	}
	if bidi.IsClosed() {
		slog.DebugContext(ctx, "muxer: dropping message for closed stream", m.msgLogFields(msg)...)
		return
	}
	if bidi.OfferWithin(msg.Payload, defaultStreamRecvOfferTimeout) {
		return
	}
	if bidi.IsClosed() {
		slog.DebugContext(ctx, "muxer: dropping message for closed stream after waiting", m.msgLogFields(msg)...)
		return
	}

	slog.WarnContext(ctx, "muxer: stream receive buffer overloaded; closing channel",
		m.msgLogFields(msg,
			slog.String("error_type", classifyTransportError(ErrStreamOverloaded)),
			slog.Duration("offer_timeout", defaultStreamRecvOfferTimeout))...)
	m.tombstoneStream(msg.ChannelID, ErrStreamOverloaded)

	if msg == nil || msg.ChannelID == uuid.Nil {
		return
	}

	go func(storeID, channelID uuid.UUID) {
		payload, err := json.Marshal(&ErrorMessage{
			Type: controlMsgSentinel + "error",
			Err:  ErrStreamOverloaded.Error(),
		})
		if err != nil {
			slog.WarnContext(m.Context, "muxer: marshal overload error failed",
				m.logFields(m.Context,
					slog.String("channel_id", channelID.String()),
					"err", err)...)
			return
		}
		if err := m.sendControl(ControlTypeError, storeID, channelID, payload); err != nil {
			slog.WarnContext(m.Context, "muxer: send overload error failed",
				m.logFields(m.Context,
					slog.String("store_id", storeID.String()),
					slog.String("channel_id", channelID.String()),
					slog.String("error_type", classifyTransportError(err)),
					"err", err)...)
		}
	}(msg.StoreID, msg.ChannelID)
}

// heartbeat periodically sends ping control frames and verifies a timely pong or activity.
func (m *WebSocketMuxer) heartbeat() {
	if m.pingInterval <= 0 {
		return
	}
	base := time.Duration(m.pingInterval) * time.Second
	jitter := time.Duration(m.pingJitter) * time.Second
	for {
		wait := m.computeHeartbeatWait(base, jitter)

		select {
		case <-time.After(wait):
			if m.checkHeartbeatTimeout() {
				return
			}
			if err := m.sendPing(); err != nil {
				m.shutdown(err)
				return
			}

		case <-m.Context.Done():
			return
		case <-m.heartbeatStop:
			return
		}
	}
}

// computeHeartbeatWait returns the duration to sleep before the next heartbeat ping.
func (m *WebSocketMuxer) computeHeartbeatWait(base, jitter time.Duration) time.Duration {
	if jitter <= 0 {
		return base
	}
	effectiveJitter := jitter
	if effectiveJitter > base {
		effectiveJitter = base
	}
	if effectiveJitter <= 0 {
		return base
	}
	delta := time.Duration(m.rng.Int63n(int64(effectiveJitter)))
	return base - delta
}

// checkHeartbeatTimeout inspects the last activity timestamp and, if the
// connection has timed out, performs best-effort close and stream shutdown.
// Returns true when it performed a timeout shutdown and the caller should return.
func (m *WebSocketMuxer) checkHeartbeatTimeout() bool {
	ts := timestamp.GetTimestamp()
	last := atomic.LoadInt64(&m.lastPongUnix)
	pongTimeoutMs := m.pongTimeout * 1000
	idleMs := ts - last
	if idleMs > pongTimeoutMs {
		atomic.AddInt64(&m.missedPongs, 1)
		slog.WarnContext(m.Context, "muxer: heartbeat timed out; closing connection",
			m.logFields(m.Context,
				slog.Int64("idle_ms", idleMs),
				slog.String("error_type", classifyTransportError(ErrHeartbeatTimeout)))...)
		// Reuse shutdown to avoid duplicating close semantics. Provide a specific
		// ErrHeartbeatTimeout so streams receive the appropriate error.
		m.shutdown(ErrHeartbeatTimeout)
		return true
	}
	return false
}

// sendControl sends a control frame (ping/pong/close/error) under the write lock.
// For data frames use sendData which already handles write locking and shutdown.
func (m *WebSocketMuxer) sendControl(control ControlType, storeID, channelID uuid.UUID, payload []byte) error {
	select {
	case <-m.done:
		return ErrMuxerClosed
	default:
	}

	pooled := m.msgPool.Get()
	if pooled == nil || m.writeQueue == nil {
		// synchronous fallback - simple sender that increments writeErrors on failure
		return m.syncSendIncrementOnError(&MuxerMsg{ControlType: control, StoreID: storeID, ChannelID: channelID, Payload: payload}, true)
	}

	msg := pooled.(*MuxerMsg)
	msg.ControlType = control
	msg.StoreID = storeID
	msg.ChannelID = channelID
	if len(payload) > 0 {
		bp := m.bufPool.Get()
		if bp != nil {
			if buf, ok := bp.([]byte); ok && cap(buf) >= len(payload) {
				b := buf[:len(payload)]
				copy(b, payload)
				msg.Payload = b
			} else {
				nb := make([]byte, len(payload))
				copy(nb, payload)
				msg.Payload = nb
			}
		} else {
			nb := make([]byte, len(payload))
			copy(nb, payload)
			msg.Payload = nb
		}
	} else {
		msg.Payload = nil
	}

	return m.enqueueWriteMessage(msg, func() error {
		return m.syncSendIncrementOnError(&MuxerMsg{ControlType: control, StoreID: storeID, ChannelID: channelID, Payload: payload}, true)
	})
}

// sendPing sends a ping control frame under the write lock. Returns an error
// if the send failed (caller is responsible for shutdown and error accounting).
func (m *WebSocketMuxer) sendPing() error {
	atomic.AddInt64(&m.pingsSent, 1)
	// try enqueue first
	pooled := m.msgPool.Get()
	if pooled == nil || m.writeQueue == nil {
		// synchronous fallback - increment writeErrors and warn on failure
		return m.sendJSONWithLock(&MuxerMsg{ControlType: ControlTypePing}, false)
	}

	msg := pooled.(*MuxerMsg)
	msg.ControlType = ControlTypePing
	msg.StoreID = uuid.Nil
	msg.ChannelID = uuid.Nil
	msg.Payload = nil

	return m.enqueueWriteMessage(msg, func() error {
		m.writeMu.Lock()
		defer m.writeMu.Unlock()
		if err := m.sendJSON(m.conn, &MuxerMsg{ControlType: ControlTypePing}); err != nil {
			atomic.AddInt64(&m.writeErrors, 1)
			if benignDisconnect(err) {
				slog.DebugContext(m.Context, "muxer: ping send skipped (disconnected)",
					m.logFields(m.Context,
						slog.String("error_type", classifyTransportError(err)),
						slog.String("error", err.Error()))...)
			} else {
				slog.WarnContext(m.Context, "muxer: ping send failed",
					m.logFields(m.Context,
						slog.String("error_type", classifyTransportError(err)),
						slog.String("error", err.Error()))...)
			}
			return err
		}
		return nil
	})
}

// sendJSONWithLock performs a synchronous JSON send under m.writeMu and
// optionally triggers m.shutdown on error. It increments writeErrors on
// failure. This consolidates repeated
// send+log+shutdown patterns used across the muxer.
func (m *WebSocketMuxer) sendJSONWithLock(msg *MuxerMsg, shutdownOnErr bool) error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()

	// fast-path: if done is closed, fail fast
	select {
	case <-m.done:
		return ErrMuxerClosed
	default:
	}

	err := m.sendJSON(m.conn, msg)
	if err != nil {
		atomic.AddInt64(&m.writeErrors, 1)
		fields := m.buildSendErrorFields(err, msg)
		if benignDisconnect(err) {
			slog.DebugContext(m.Context, "muxer: write/send stopped (disconnected)", fields...)
		} else {
			slog.ErrorContext(m.Context, "muxer: write/send failed", fields...)
		}
		if shutdownOnErr {
			// Do not call shutdown while holding writeMu: shutdown locks writeMu and would deadlock.
			go m.shutdown(err)
		}
		return err
	}

	return nil
}

// buildSendErrorFields creates structured log fields for send errors. Extracted
// to simplify sendJSONWithLock and centralize op/net error unwrapping.
func (m *WebSocketMuxer) buildSendErrorFields(err error, msg *MuxerMsg) []any {
	var (
		netErr net.Error
		opErr  *net.OpError
	)
	fields := []any{
		slog.String("muxer", m.name),
		slog.String("error_type", classifyTransportError(err)),
	}
	if msg != nil {
		fields = append(fields, slog.String("channel_id", msg.ChannelID.String()))
		fields = append(fields, slog.Int("bytes", len(msg.Payload)))
	}
	if errors.As(err, &netErr) {
		fields = append(fields, slog.String("net_err", netErr.Error()), slog.Bool("temporary", netErr.Temporary()))
	}
	if errors.As(err, &opErr) && opErr != nil {
		if opErr.Op != "" {
			fields = append(fields, slog.String("op", opErr.Op))
		}
		if opErr.Net != "" {
			fields = append(fields, slog.String("net", opErr.Net))
		}
		if opErr.Addr != nil {
			fields = append(fields, slog.String("addr", opErr.Addr.String()))
		}
		if opErr.Err != nil {
			fields = append(fields, slog.String("op_err", opErr.Err.Error()))
		}
	}
	if err == io.EOF {
		fields = append(fields, slog.String("err", "EOF"))
	} else {
		fields = append(fields, slog.String("err", err.Error()))
	}
	return fields
}

// syncSendIncrementOnError is a thin wrapper for sendJSONWithLock that always
// increments writeErrors on failure and optionally does shutdown; it's kept for
// call-site clarity during refactor.
func (m *WebSocketMuxer) syncSendIncrementOnError(msg *MuxerMsg, shutdownOnErr bool) error {
	return m.sendJSONWithLock(msg, shutdownOnErr)
}

// releaseMsg returns a message's payload buffer and the message itself back to
// their pools, if they match expected shapes.
func (m *WebSocketMuxer) releaseMsg(msg *MuxerMsg) {
	if msg == nil {
		return
	}
	if msg.Payload != nil {
		if cap(msg.Payload) == 1024 {
			m.bufPool.Put(msg.Payload[:0])
		}
	}
	msg.ControlType = ""
	msg.StoreID = uuid.Nil
	msg.ChannelID = uuid.Nil
	msg.Payload = nil
	m.msgPool.Put(msg)
}

// writePump serializes outgoing messages to the websocket connection.
// It owns the json.Encoder and returns message objects to the pool after send.
func (m *WebSocketMuxer) writePump() {
	defer func() {
		if m.writerDone != nil {
			close(m.writerDone)
		}
	}()

	for {
		select {
		case <-m.done:
			// shutdown signalled: drain any in-flight messages without blocking
			for {
				select {
				case msg := <-m.writeQueue:
					if msg == nil {
						continue
					}
					m.recordWriteQueueDrain()
					// ensure pooled resources are released regardless of send outcome
					if ok := func(msg *MuxerMsg) bool {
						defer m.releaseMsg(msg)
						if err := m.sendJSONWithLock(msg, true); err != nil {
							// sendJSONWithLock already logged and shutdown when requested
							return false
						}
						return true
					}(msg); !ok {
						return
					}
				default:
					// queue drained, exit
					return
				}
			}
		case msg := <-m.writeQueue:
			if msg == nil {
				continue
			}
			m.recordWriteQueueDrain()
			// ensure pooled resources are released regardless of send outcome
			if ok := func(msg *MuxerMsg) bool {
				defer m.releaseMsg(msg)
				if err := m.sendJSONWithLock(msg, true); err != nil {
					return false
				}
				return true
			}(msg); !ok {
				return
			}
		}
	}
}

// shutdown performs an orderly shutdown of the muxer and all streams.
func (m *WebSocketMuxer) shutdown(reason error) {
	m.closeOnce.Do(func() {
		// best-effort notify peer of close: signal done so senders stop and
		// writePump can drain remaining messages. Do NOT close m.writeQueue here
		// because concurrent senders may still be racing to enqueue and that
		// would cause a send-on-closed-channel panic.
		select {
		case <-m.done:
			// already closed
		default:
			close(m.done)
		}
		// wait for writer to finish (best-effort) if writerDone is available
		if m.writerDone != nil {
			select {
			case <-m.writerDone:
			case <-time.After(2 * time.Second):
			}
		}
		if m.conn != nil {
			// Issue #22: Acquire writeMu lock before final write to prevent concurrent writes.
			// Without this lock, if a goroutine is still writing, the frame gets corrupted.
			m.writeMu.Lock()
			if err := m.sendJSON(m.conn, &MuxerMsg{ControlType: ControlTypeClose}); err != nil {
				if benignDisconnect(err) {
					slog.DebugContext(m.Context, "muxer: shutdown close frame not sent (already disconnected)",
						m.logFields(m.Context,
							slog.String("error_type", classifyTransportError(err)),
							"err", err)...)
				} else {
					slog.WarnContext(m.Context, "muxer: shutdown close frame send failed",
						m.logFields(m.Context,
							slog.String("error_type", classifyTransportError(err)),
							"err", err)...)
				}
			}
			m.writeMu.Unlock()
			if err := m.conn.Close(); err != nil {
				if benignDisconnect(err) {
					slog.DebugContext(m.Context, "muxer: shutdown conn close skipped",
						m.logFields(m.Context,
							slog.String("error_type", classifyTransportError(err)),
							"err", err)...)
				} else {
					slog.WarnContext(m.Context, "muxer: shutdown conn close failed",
						m.logFields(m.Context,
							slog.String("error_type", classifyTransportError(err)),
							"err", err)...)
				}
			}
		}
		if m.cancelFunc != nil {
			m.cancelFunc()
		}
		// signal heartbeat to stop if running
		if m.heartbeatStop != nil {
			select {
			case <-m.heartbeatStop:
				// already closed
			default:
				close(m.heartbeatStop)
			}
		}
		// m.done already closed above to prevent races with writers

		// Snapshot channels under read lock, then close outside the lock.
		// Holding RLock while calling CloseLocal would deadlock because CloseLocal
		// triggers the cleanup closure which acquires channelsMu.Lock (not reentrant).
		m.channelsMu.RLock()
		streams := make([]*MuxerBidiStream, 0, len(m.channels))
		for _, s := range m.channels {
			streams = append(streams, s)
		}
		m.channelsMu.RUnlock()

		for _, s := range streams {
			if reason != nil {
				s.CloseLocal(reason)
			} else {
				s.CloseLocal(nil)
			}
		}
	})
}

// Close shuts down the muxer. It satisfies the providerMuxer interface and
// delegates to shutdown so callers need not type-assert to *WebSocketMuxer.
// Before shutting down it waits (up to 1 s) for any in-flight streams to
// complete, replacing the heuristic 500 ms sleep in replaceMuxer/invalidateMuxer.
func (m *WebSocketMuxer) Close(err error) {
	// Wait for in-flight streams to finish. Poll every 10 ms, bail after 1 s.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&m.activeStreams) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	m.shutdown(err)
}

// Metrics accessors (atomic snapshots)
func (m *WebSocketMuxer) PingsSent() int64     { return atomic.LoadInt64(&m.pingsSent) }
func (m *WebSocketMuxer) PongsReceived() int64 { return atomic.LoadInt64(&m.pongsReceived) }
func (m *WebSocketMuxer) MissedPongs() int64   { return atomic.LoadInt64(&m.missedPongs) }
func (m *WebSocketMuxer) WriteErrors() int64   { return atomic.LoadInt64(&m.writeErrors) }
func (m *WebSocketMuxer) WriteQueueBlocks() int64 {
	return atomic.LoadInt64(&m.writeQueueBlocks)
}
func (m *WebSocketMuxer) WriteQueueDepth() int64 {
	return atomic.LoadInt64(&m.writeQueueDepth)
}
func (m *WebSocketMuxer) WriteQueueFallbacks() int64 {
	return atomic.LoadInt64(&m.writeQueueFallbacks)
}
func (m *WebSocketMuxer) WriteQueueSaturationWarnings() int64 {
	return atomic.LoadInt64(&m.writeQueueSaturationWarnings)
}
