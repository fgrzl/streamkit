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
type MuxerMsg struct {
	ControlType ControlType `type:"control_type"`
	StoreID     uuid.UUID   `json:"store_id"`
	ChannelID   uuid.UUID   `json:"channel_id"`
	Payload     []byte      `json:"payload"`
}

// WebSocketMuxer multiplexes multiple logical bidirectional streams over a single WebSocket connection.
// Each logical stream is identified by a ChannelID.
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
	// heartbeat configuration
	pingInterval  int64 // seconds
	pongTimeout   int64 // seconds
	lastPongUnix  int64 // unix seconds of last received pong or activity (atomic access)
	heartbeatStop chan struct{}
	cancelFunc    context.CancelFunc
	pingJitter    int64 // seconds of jitter +/- around pingInterval
	// runtime counters
	pingsSent     int64
	pingsReceived int64
	pongsReceived int64
	missedPongs   int64
	writeErrors   int64
	logger        *slog.Logger
	rng           *rand.Rand
	// debug log rate limiting (unix seconds)
	lastPingLogUnix  int64
	lastPongLogUnix  int64
	debugLogInterval int64 // seconds; 0 disables throttling
}

// sentinel errors
var (
	ErrMuxerClosed      = errors.New("muxer closed")
	ErrHeartbeatTimeout = errors.New("heartbeat timeout")
)

// NewClientWebSocketMuxer will spawn a read loop as a go routine and returns the *WebSocketMuxer
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
	m.logger = slog.With(slog.String("muxer", m.name))
	// per-muxer logger with common fields
	m.Context = cctx
	// per-muxer RNG for jittered heartbeat
	m.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	// initialize lastPongUnix atomically
	atomic.StoreInt64(&m.lastPongUnix, timestamp.GetTimestamp())
	go m.readLoop()
	go m.heartbeat()
	return m
}

// NewServerWebSocketMuxer will start a blocking read loop to keep the websocket connection open
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
	// heartbeat should run concurrently
	m.Context = cctx
	m.logger = slog.With(slog.String("muxer", m.name))
	m.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	// initialize lastPongUnix atomically
	atomic.StoreInt64(&m.lastPongUnix, timestamp.GetTimestamp())
	go m.heartbeat()
	// readLoop blocks the caller (the websocket handler) until the connection closes
	m.readLoop()
}

func (m *WebSocketMuxer) Ping() bool {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	if websocket.JSON.Send(m.conn, &MuxerMsg{ControlType: ControlTypePing}) == nil {
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
func (m *WebSocketMuxer) Register(storeID, channelID uuid.UUID) api.BidiStream {
	return m.register(storeID, channelID)
}

// internal registration logic (safe for reuse)
func (m *WebSocketMuxer) register(storeID, channelID uuid.UUID) *MuxerBidiStream {
	sendFn := func(payload []byte) error {
		m.writeMu.Lock()
		defer m.writeMu.Unlock()
		// if done is closed, fail fast
		select {
		case <-m.done:
			return ErrMuxerClosed
		default:
		}
		err := websocket.JSON.Send(m.conn, &MuxerMsg{
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

// readLoop continuously receives messages from the WebSocket,
// routes them to the appropriate stream, and auto-registers new streams.
func (m *WebSocketMuxer) readLoop() {
	for {
		var msg MuxerMsg
		if err := websocket.JSON.Receive(m.conn, &msg); err != nil {
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
			return
		}

		// refresh activity time on any received message
		atomic.StoreInt64(&m.lastPongUnix, timestamp.GetTimestamp())

		ctx := node.WithChannelID(m.Context, msg.ChannelID)

		switch msg.ControlType {
		case ControlTypePing:
			// increment counter and rate-limit debug log
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
			_ = websocket.JSON.Send(m.conn, &MuxerMsg{
				ControlType: ControlTypePong,
			})
			// refresh activity
			atomic.StoreInt64(&m.lastPongUnix, timestamp.GetTimestamp())
			continue

		case ControlTypePong:
			// increment counter and rate-limit debug log
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
			continue

		case ControlTypeClose:
			slog.InfoContext(ctx, "muxer: received close")
			m.shutdown(nil)
			return

		case ControlTypeError:
			slog.WarnContext(ctx, "muxer: received error control message",
				slog.String("store_id", msg.StoreID.String()),
				slog.String("channel_id", msg.ChannelID.String()),
				slog.String("payload", string(msg.Payload)),
			)
			continue

		case ControlTypeData:
			if !m.session.CanAccessStore(msg.StoreID) {
				payload, _ := json.Marshal(&ErrorMessage{
					Type: "err",
					Err:  "access denied",
				})

				_ = websocket.JSON.Send(m.conn, &MuxerMsg{
					ControlType: ControlTypeError,
					StoreID:     msg.StoreID,
					ChannelID:   msg.ChannelID,
					Payload:     payload,
				})
				continue
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
					continue
				}

				// Acquire node instance before registering stream; if this fails, notify client and skip registering.
				instance, err := m.nodeManager.GetOrCreate(ctx, msg.StoreID)
				if err != nil {
					slog.ErrorContext(ctx, "muxer: failed to get or create node",
						slog.String("store_id", msg.StoreID.String()),
						slog.String("err", err.Error()))
					// Inform client that the store/node is unavailable
					payload, _ := json.Marshal(&ErrorMessage{Type: "error", Err: "store unavailable"})
					_ = websocket.JSON.Send(m.conn, &MuxerMsg{
						ControlType: ControlTypeError,
						StoreID:     msg.StoreID,
						ChannelID:   msg.ChannelID,
						Payload:     payload,
					})
					continue
				}

				bidi = m.register(msg.StoreID, msg.ChannelID)
				if bidi != nil {
					// Start the instance handler only after successful registration.
					go instance.Handle(ctx, bidi)
				} else {
					slog.WarnContext(ctx, "muxer: stream registration returned nil bidi", slog.String("channel_id", msg.ChannelID.String()))
				}
			}

			if bidi == nil {
				// Optionally, notify client or log error here.
				slog.ErrorContext(ctx, "muxer: cannot deliver message, bidi is nil", slog.String("channel_id", msg.ChannelID.String()))
				continue
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

		default:
			slog.WarnContext(ctx, "muxer: unrecognized control type",
				slog.String("type", string(msg.ControlType)))
			continue
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
		// compute randomized wait: (base - effectiveJitter, base]
		// Cap the jitter to base so wait never becomes negative. If effectiveJitter
		// is zero or negative, fall back to base interval.
		var wait time.Duration
		if jitter > 0 {
			effectiveJitter := jitter
			if effectiveJitter > base {
				effectiveJitter = base
			}
			if effectiveJitter <= 0 {
				wait = base
			} else {
				// choose a random amount in [0, effectiveJitter) and subtract from base
				delta := time.Duration(m.rng.Int63n(int64(effectiveJitter)))
				wait = base - delta
			}
		} else {
			wait = base
		}

		select {
		case <-time.After(wait):
			ts := timestamp.GetTimestamp()
			last := atomic.LoadInt64(&m.lastPongUnix)
			if ts-last > m.pongTimeout {
				atomic.AddInt64(&m.missedPongs, 1)
				slog.WarnContext(m.Context, "muxer: heartbeat timed out; closing connection", slog.Int64("idle_seconds", ts-last))
				m.closeOnce.Do(func() {
					// best-effort notify peer
					_ = websocket.JSON.Send(m.conn, &MuxerMsg{ControlType: ControlTypeClose})
					_ = m.conn.Close()
					if m.cancelFunc != nil {
						m.cancelFunc()
					}
					select {
					case <-m.done:
					default:
						close(m.done)
					}
					m.channelsMu.RLock()
					for _, s := range m.channels {
						s.CloseLocal(ErrHeartbeatTimeout)
					}
					m.channelsMu.RUnlock()
				})
				return
			}

			atomic.AddInt64(&m.pingsSent, 1)
			m.writeMu.Lock()
			if err := websocket.JSON.Send(m.conn, &MuxerMsg{ControlType: ControlTypePing}); err != nil {
				atomic.AddInt64(&m.writeErrors, 1)
				slog.WarnContext(m.Context, "muxer: ping send failed", slog.String("error", err.Error()))
				m.writeMu.Unlock()
				m.shutdown(err)
				return
			}
			m.writeMu.Unlock()

		case <-m.Context.Done():
			return
		case <-m.heartbeatStop:
			return
		}
	}
}

// shutdown performs an orderly shutdown of the muxer and all streams.
func (m *WebSocketMuxer) shutdown(reason error) {
	m.closeOnce.Do(func() {
		// best-effort notify peer of close
		_ = websocket.JSON.Send(m.conn, &MuxerMsg{ControlType: ControlTypeClose})
		// best-effort close websocket
		_ = m.conn.Close()
		if m.cancelFunc != nil {
			m.cancelFunc()
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
