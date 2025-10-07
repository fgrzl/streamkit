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

	"github.com/fgrzl/timestamp"

	"sync/atomic"

	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/node"
	"github.com/google/uuid"
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
type MuxerMsg struct {
	ControlType ControlType `type:"control_type"`
	StoreID     uuid.UUID   `json:"store_id"`
	ChannelID   uuid.UUID   `json:"channel_id"`
	Payload     []byte      `json:"payload"`
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
	channelsMu  sync.RWMutex
	writeMu     sync.Mutex
	done        chan struct{}
	nodeManager node.NodeManager
	closeOnce   sync.Once

	// outbound write pump
	writeQueue     chan *MuxerMsg
	writeQueueSize int
	msgPool        sync.Pool
	bufPool        sync.Pool
	writerDone     chan struct{}

	// Heartbeat configuration (seconds)
	pingInterval  int64
	pongTimeout   int64
	lastPongUnix  int64 // atomic
	heartbeatStop chan struct{}
	cancelFunc    context.CancelFunc
	pingJitter    int64

	// runtime counters
	pingsSent     int64
	pingsReceived int64
	pongsReceived int64
	missedPongs   int64
	writeErrors   int64
	logger        *slog.Logger
	rng           *rand.Rand
	// JSON send/receive hooks (set to websocket.JSON.Send/Receive by default).
	// Tests may override these to simulate network behavior.
	sendJSON func(conn *websocket.Conn, v interface{}) error
	recvJSON func(conn *websocket.Conn, v interface{}) error

	// debug log rate limiting (unix seconds); 0 disables throttling
	lastPingLogUnix  int64
	lastPongLogUnix  int64
	debugLogInterval int64
}

// sentinel errors
var (
	ErrMuxerClosed      = errors.New("muxer closed")
	ErrHeartbeatTimeout = errors.New("heartbeat timeout")
)

// NewClientWebSocketMuxer will spawn a read loop as a go routine and returns the *WebSocketMuxer
// NewClientWebSocketMuxer creates a client-side muxer, starts its read loop
// and heartbeat goroutines, and returns the muxer instance.
func NewClientWebSocketMuxer(ctx context.Context, session MuxerSession, conn *websocket.Conn) *WebSocketMuxer {
	cctx, cancel := context.WithCancel(ctx)
	m := &WebSocketMuxer{
		Context:          ctx,
		session:          session,
		name:             "client",
		conn:             conn,
		channels:         make(map[uuid.UUID]*MuxerBidiStream),
		done:             make(chan struct{}),
		pingInterval:     30,
		pongTimeout:      90,
		pingJitter:       5,
		lastPongUnix:     timestamp.GetTimestamp(),
		heartbeatStop:    make(chan struct{}),
		cancelFunc:       cancel,
		debugLogInterval: 60,
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
	go m.readLoop()
	go m.heartbeat()
	go m.writePump()
	return m
}

// NewServerWebSocketMuxer will start a blocking read loop to keep the websocket connection open
// NewServerWebSocketMuxer constructs a server-side muxer, starts the
// heartbeat goroutine and blocks in readLoop until the connection closes.
func NewServerWebSocketMuxer(ctx context.Context, session MuxerSession, nodeManager node.NodeManager, conn *websocket.Conn) {
	cctx, cancel := context.WithCancel(ctx)
	m := &WebSocketMuxer{
		Context:          cctx,
		session:          session,
		name:             "server",
		conn:             conn,
		nodeManager:      nodeManager,
		channels:         make(map[uuid.UUID]*MuxerBidiStream),
		done:             make(chan struct{}),
		pingInterval:     30,
		pongTimeout:      90,
		pingJitter:       5,
		lastPongUnix:     timestamp.GetTimestamp(),
		heartbeatStop:    make(chan struct{}),
		cancelFunc:       cancel,
		debugLogInterval: 60,
	}
	m.Context = cctx
	m.logger = slog.With(slog.String("muxer", m.name))
	m.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	atomic.StoreInt64(&m.lastPongUnix, timestamp.GetTimestamp())
	m.sendJSON = func(conn *websocket.Conn, v interface{}) error { return websocket.JSON.Send(conn, v) }
	m.recvJSON = func(conn *websocket.Conn, v interface{}) error { return websocket.JSON.Receive(conn, v) }
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

// Ping sends a control ping and returns true on success. It uses the same
// metrics and locking as the heartbeat's ping path.
func (m *WebSocketMuxer) Ping() bool {
	if err := m.sendPing(); err == nil {
		return true
	}
	return false
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
	return m.register(storeID, channelID)
}

// internal registration logic (safe for reuse)
func (m *WebSocketMuxer) register(storeID, channelID uuid.UUID) *MuxerBidiStream {
	sendFn := func(payload []byte) error { return m.sendData(storeID, channelID, payload) }

	cleanup := func() {
		m.channelsMu.Lock()
		defer m.channelsMu.Unlock()
		delete(m.channels, channelID)
		slog.DebugContext(m.Context, "muxer: stream unregistered", slog.String("channel_id", channelID.String()))
	}

	bidi := NewMuxerBidiStream(sendFn, cleanup)
	bidi.SetChannelID(channelID)

	m.channelsMu.Lock()
	m.channels[channelID] = bidi
	m.channelsMu.Unlock()

	slog.DebugContext(m.Context, "muxer: stream registered", slog.String("channel_id", channelID.String()))
	return bidi
}

// sendData sends a data control frame for the given store/channel and handles
// error reporting and shutdown logic. It mirrors the behavior previously
// embedded inline within register.
func (m *WebSocketMuxer) sendData(storeID, channelID uuid.UUID, payload []byte) error {
	// fast-fail if muxer is closed
	select {
	case <-m.done:
		return ErrMuxerClosed
	default:
	}

	// prepare pooled message (if available)
	pooled := m.msgPool.Get()
	if pooled == nil || m.writeQueue == nil {
		// fallback to original synchronous behavior when pool or queue not initialized
		m.writeMu.Lock()
		defer m.writeMu.Unlock()
		// if done is closed, fail fast
		select {
		case <-m.done:
			return ErrMuxerClosed
		default:
		}

		err := m.sendJSON(m.conn, &MuxerMsg{
			ControlType: ControlTypeData,
			StoreID:     storeID,
			ChannelID:   channelID,
			Payload:     payload,
		})
		if err != nil {
			var (
				netErr net.Error
				opErr  *net.OpError
			)
			fields := []any{slog.String("channel_id", channelID.String()), slog.Int("bytes", len(payload))}
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
			slog.ErrorContext(m.Context, "muxer: write/send failed", fields...)
			m.shutdown(err)
		} else {
			atomic.StoreInt64(&m.lastPongUnix, timestamp.GetTimestamp())
		}
		return err
	}

	msg := pooled.(*MuxerMsg)
	msg.ControlType = ControlTypeData
	msg.StoreID = storeID
	msg.ChannelID = channelID
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

	// try enqueue without blocking; fall back to synchronous write to preserve semantics
	select {
	case m.writeQueue <- msg:
		return nil
	default:
		// queue full; return msg to pool and perform synchronous send under lock
		m.msgPool.Put(msg)
		m.writeMu.Lock()
		defer m.writeMu.Unlock()
		// check closed again
		select {
		case <-m.done:
			return ErrMuxerClosed
		default:
		}

		err := m.sendJSON(m.conn, &MuxerMsg{
			ControlType: ControlTypeData,
			StoreID:     storeID,
			ChannelID:   channelID,
			Payload:     payload,
		})
		if err != nil {
			// On write error, perform orderly shutdown of the muxer and affected streams.
			// Unwrap common net errors for improved diagnostics.
			var (
				netErr net.Error
				opErr  *net.OpError
			)
			fields := []any{slog.String("channel_id", channelID.String()), slog.Int("bytes", len(payload))}
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
			// Always include the raw error string for context.
			if err == io.EOF {
				fields = append(fields, slog.String("err", "EOF"))
			} else {
				fields = append(fields, slog.String("err", err.Error()))
			}
			slog.ErrorContext(m.Context, "muxer: write/send failed", fields...)
			m.shutdown(err)
		} else {
			// mark activity on successful send
			atomic.StoreInt64(&m.lastPongUnix, timestamp.GetTimestamp())
		}
		return err
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

		// refresh activity time on any received message
		atomic.StoreInt64(&m.lastPongUnix, timestamp.GetTimestamp())

		// Delegate control-type handling to a helper to keep the loop body small.
		m.processMessage(&msg)
	}
}

// handleReceiveError centralizes logging and shutdown behavior when websocket.Receive fails.
func (m *WebSocketMuxer) handleReceiveError(err error) {
	// Treat EOF as a normal, peer-initiated close
	if err == io.EOF {
		slog.InfoContext(m.Context, "muxer: websocket closed by peer")
		m.shutdown(nil)
		return
	}
	// Unwrap net errors for richer logs
	var (
		netErr net.Error
		opErr  *net.OpError
	)
	fields := []any{}
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

// processMessage handles a single decoded MuxerMsg. It contains the large
// control-type switch extracted from readLoop so the loop itself stays concise.
func (m *WebSocketMuxer) processMessage(msg *MuxerMsg) {
	ctx := node.WithChannelID(m.Context, msg.ChannelID)
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
			slog.String("type", string(msg.ControlType)))
	}
}

func (m *WebSocketMuxer) handlePing(ctx context.Context) {
	ts := timestamp.GetTimestamp()
	total := atomic.AddInt64(&m.pingsReceived, 1)
	if m.logger.Enabled(context.Background(), slog.LevelDebug) {
		if m.debugLogInterval <= 0 {
			m.logger.DebugContext(ctx, "muxer: received ping", slog.Int64("pings_total", total))
		} else {
			last := atomic.LoadInt64(&m.lastPingLogUnix)
			if ts-last >= m.debugLogInterval && atomic.CompareAndSwapInt64(&m.lastPingLogUnix, last, ts) {
				m.logger.DebugContext(ctx, "muxer: received ping", slog.Int64("pings_total", total))
			}
		}
	}
	_ = m.sendControl(ControlTypePong, uuid.Nil, uuid.Nil, nil)
	atomic.StoreInt64(&m.lastPongUnix, timestamp.GetTimestamp())
}

func (m *WebSocketMuxer) handlePong(ctx context.Context) {
	ts := timestamp.GetTimestamp()
	total := atomic.AddInt64(&m.pongsReceived, 1)
	if m.logger.Enabled(context.Background(), slog.LevelDebug) {
		if m.debugLogInterval <= 0 {
			m.logger.DebugContext(ctx, "muxer: received pong", slog.Int64("pongs_total", total))
		} else {
			last := atomic.LoadInt64(&m.lastPongLogUnix)
			if ts-last >= m.debugLogInterval && atomic.CompareAndSwapInt64(&m.lastPongLogUnix, last, ts) {
				m.logger.DebugContext(ctx, "muxer: received pong", slog.Int64("pongs_total", total))
			}
		}
	}
	atomic.StoreInt64(&m.lastPongUnix, ts)
}

func (m *WebSocketMuxer) handleClose(ctx context.Context) {
	slog.InfoContext(ctx, "muxer: received close")
	m.shutdown(nil)
}

func (m *WebSocketMuxer) handleErrorMessage(ctx context.Context, msg *MuxerMsg) {
	slog.WarnContext(ctx, "muxer: received error control message",
		slog.String("store_id", msg.StoreID.String()),
		slog.String("channel_id", msg.ChannelID.String()),
		slog.String("payload", string(msg.Payload)),
	)
}

func (m *WebSocketMuxer) handleDataMessage(ctx context.Context, msg *MuxerMsg) {
	if !m.session.CanAccessStore(msg.StoreID) {
		payload, _ := json.Marshal(&ErrorMessage{Type: "err", Err: "access denied"})
		_ = m.sendControl(ControlTypeError, msg.StoreID, msg.ChannelID, payload)
		return
	}

	m.channelsMu.RLock()
	bidi, exists := m.channels[msg.ChannelID]
	m.channelsMu.RUnlock()

	if !exists {
		if m.nodeManager == nil {
			if m.name == "server" {
				slog.ErrorContext(ctx, "muxer: node manager is nil on server side")
				return
			}
			return
		}

		instance, err := m.nodeManager.GetOrCreate(ctx, msg.StoreID)
		if err != nil {
			slog.ErrorContext(ctx, "muxer: failed to get or create node",
				slog.String("store_id", msg.StoreID.String()),
				slog.String("err", err.Error()))
			payload, _ := json.Marshal(&ErrorMessage{Type: "error", Err: "store unavailable"})
			_ = m.sendControl(ControlTypeError, msg.StoreID, msg.ChannelID, payload)
			return
		}

		bidi = m.register(msg.StoreID, msg.ChannelID)
		if bidi != nil {
			go instance.Handle(ctx, bidi)
		} else {
			slog.WarnContext(ctx, "muxer: stream registration returned nil bidi", slog.String("channel_id", msg.ChannelID.String()))
		}
	}

	if bidi == nil {
		slog.ErrorContext(ctx, "muxer: cannot deliver message, bidi is nil", slog.String("channel_id", msg.ChannelID.String()))
		return
	}
	if bidi.Offer(msg.Payload) {
		if m.logger.Enabled(context.Background(), slog.LevelDebug) {
			slog.DebugContext(ctx, "muxer: delivered message",
				slog.String("channel_id", msg.ChannelID.String()))
		}
	} else {
		if m.logger.Enabled(context.Background(), slog.LevelDebug) {
			slog.DebugContext(ctx, "muxer: dropped message for closed stream",
				slog.String("channel_id", msg.ChannelID.String()))
		}
	}
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
	if ts-last > m.pongTimeout {
		atomic.AddInt64(&m.missedPongs, 1)
		slog.WarnContext(m.Context, "muxer: heartbeat timed out; closing connection", slog.Int64("idle_seconds", ts-last))
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
		// synchronous fallback
		m.writeMu.Lock()
		defer m.writeMu.Unlock()
		if err := m.sendJSON(m.conn, &MuxerMsg{
			ControlType: control,
			StoreID:     storeID,
			ChannelID:   channelID,
			Payload:     payload,
		}); err == nil {
			atomic.StoreInt64(&m.lastPongUnix, timestamp.GetTimestamp())
			return nil
		} else {
			atomic.AddInt64(&m.writeErrors, 1)
			return err
		}
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

	select {
	case m.writeQueue <- msg:
		return nil
	default:
		// queue full -> synchronous fallback
		m.msgPool.Put(msg)
		m.writeMu.Lock()
		defer m.writeMu.Unlock()
		if err := m.sendJSON(m.conn, &MuxerMsg{
			ControlType: control,
			StoreID:     storeID,
			ChannelID:   channelID,
			Payload:     payload,
		}); err == nil {
			atomic.StoreInt64(&m.lastPongUnix, timestamp.GetTimestamp())
			return nil
		} else {
			atomic.AddInt64(&m.writeErrors, 1)
			return err
		}
	}
}

// sendPing sends a ping control frame under the write lock. Returns an error
// if the send failed (caller is responsible for shutdown and error accounting).
func (m *WebSocketMuxer) sendPing() error {
	atomic.AddInt64(&m.pingsSent, 1)
	// try enqueue first
	pooled := m.msgPool.Get()
	if pooled == nil || m.writeQueue == nil {
		// synchronous fallback
		m.writeMu.Lock()
		defer m.writeMu.Unlock()
		if err := m.sendJSON(m.conn, &MuxerMsg{ControlType: ControlTypePing}); err != nil {
			atomic.AddInt64(&m.writeErrors, 1)
			slog.WarnContext(m.Context, "muxer: ping send failed", slog.String("error", err.Error()))
			return err
		}
		return nil
	}

	msg := pooled.(*MuxerMsg)
	msg.ControlType = ControlTypePing
	msg.StoreID = uuid.Nil
	msg.ChannelID = uuid.Nil
	msg.Payload = nil

	select {
	case m.writeQueue <- msg:
		return nil
	default:
		m.msgPool.Put(msg)
		m.writeMu.Lock()
		defer m.writeMu.Unlock()
		if err := m.sendJSON(m.conn, &MuxerMsg{ControlType: ControlTypePing}); err != nil {
			atomic.AddInt64(&m.writeErrors, 1)
			slog.WarnContext(m.Context, "muxer: ping send failed", slog.String("error", err.Error()))
			return err
		}
		return nil
	}
}

// writePump serializes outgoing messages to the websocket connection.
// It owns the json.Encoder and returns message objects to the pool after send.
func (m *WebSocketMuxer) writePump() {
	defer func() {
		if m.writerDone != nil {
			close(m.writerDone)
		}
	}()

	for msg := range m.writeQueue {
		if msg == nil {
			continue
		}
		// perform send using encoder if available, otherwise fall back to sendJSON
		// Serialize all encoder/send calls with writeMu to avoid concurrent writes
		var err error
		m.writeMu.Lock()
		err = m.sendJSON(m.conn, msg)
		m.writeMu.Unlock()

		if err != nil {
			atomic.AddInt64(&m.writeErrors, 1)
			// best-effort detailed logging
			slog.WarnContext(m.Context, "muxer: write pump send failed", slog.String("err", err.Error()))
			m.shutdown(err)
			// return early; shutdown will close m.done and trigger other cleanup
			return
		}
		// mark activity
		atomic.StoreInt64(&m.lastPongUnix, timestamp.GetTimestamp())

		// release payload buffer back to pool if it matches expected size
		if msg.Payload != nil {
			if cap(msg.Payload) == 1024 {
				// zero-length slice with capacity preserved
				m.bufPool.Put(msg.Payload[:0])
			}
		}
		// release msg back to pool (clear fields)
		msg.ControlType = ""
		msg.StoreID = uuid.Nil
		msg.ChannelID = uuid.Nil
		msg.Payload = nil
		m.msgPool.Put(msg)
	}
}

// shutdown performs an orderly shutdown of the muxer and all streams.
func (m *WebSocketMuxer) shutdown(reason error) {
	m.closeOnce.Do(func() {
		// best-effort notify peer of close: close the write queue and wait for writer
		if m.writeQueue != nil {
			// close queue so writePump finishes
			close(m.writeQueue)
			// wait for writer to finish (best-effort) if writerDone is available
			if m.writerDone != nil {
				select {
				case <-m.writerDone:
				case <-time.After(2 * time.Second):
				}
			}
		}
		if m.conn != nil {
			// try a final write without locking - best effort
			_ = m.sendJSON(m.conn, &MuxerMsg{ControlType: ControlTypeClose})
			_ = m.conn.Close()
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
		// close done if not already closed
		select {
		case <-m.done:
			// already closed
		default:
			close(m.done)
		}

		// close streams locally
		m.channelsMu.RLock()
		for _, s := range m.channels {
			if reason != nil {
				s.CloseLocal(reason)
			} else {
				s.CloseLocal(nil)
			}
		}
		m.channelsMu.RUnlock()
	})
}

// Metrics accessors (atomic snapshots)
func (m *WebSocketMuxer) PingsSent() int64     { return atomic.LoadInt64(&m.pingsSent) }
func (m *WebSocketMuxer) PongsReceived() int64 { return atomic.LoadInt64(&m.pongsReceived) }
func (m *WebSocketMuxer) MissedPongs() int64   { return atomic.LoadInt64(&m.missedPongs) }
func (m *WebSocketMuxer) WriteErrors() int64   { return atomic.LoadInt64(&m.writeErrors) }
