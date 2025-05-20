package wskit

import (
	"context"
	"log/slog"
	"sync"

	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/node"
	"github.com/google/uuid"
	"golang.org/x/net/websocket"
)

// MuxerMsg represents a framed message sent over the multiplexed WebSocket.
// Each message is scoped to a specific logical channel by ChannelID.
type MuxerMsg struct {
	ChannelID uuid.UUID `json:"channel_id"`
	Payload   []byte    `json:"payload"`
}

// WebSocketMuxer multiplexes multiple logical bidirectional streams over a single WebSocket connection.
// Each logical stream is identified by a ChannelID.
type WebSocketMuxer struct {
	Context    context.Context
	conn       *websocket.Conn
	channels   map[uuid.UUID]*MuxerBidiStream
	channelsMu sync.RWMutex
	writeMu    sync.Mutex
	done       chan struct{}
	node       node.Node
}

// NewWebSocketMuxer wraps a websocket.Conn and starts its internal reader loop.
func NewWebSocketMuxer(ctx context.Context, conn *websocket.Conn) *WebSocketMuxer {
	m := &WebSocketMuxer{
		Context:  ctx,
		conn:     conn,
		channels: make(map[uuid.UUID]*MuxerBidiStream),
		done:     make(chan struct{}),
	}
	go m.readLoop()
	return m
}

type Handler = func(context.Context, api.BidiStream)

func NewServerWebSocketMuxer(conn *websocket.Conn, node node.Node) {
	m := &WebSocketMuxer{
		conn:     conn,
		node:     node,
		channels: make(map[uuid.UUID]*MuxerBidiStream),
		done:     make(chan struct{}),
	}
	m.readLoop()
}

// Serve blocks until the WebSocket connection is closed or an error occurs.
func (m *WebSocketMuxer) Serve() {
	<-m.done
}

// Register creates and tracks a new stream for the given ChannelID.
// If a stream with this ID already exists, it is overwritten.
func (m *WebSocketMuxer) Register(channelID uuid.UUID) api.BidiStream {
	return m.register(channelID)
}

// internal registration logic (safe for reuse)
func (m *WebSocketMuxer) register(channelID uuid.UUID) *MuxerBidiStream {
	sendFn := func(payload []byte) error {
		m.writeMu.Lock()
		defer m.writeMu.Unlock()
		return websocket.JSON.Send(m.conn, &MuxerMsg{
			ChannelID: channelID,
			Payload:   payload,
		})
	}

	cleanup := func() {
		m.channelsMu.Lock()
		defer m.channelsMu.Unlock()
		delete(m.channels, channelID)
		slog.Debug("muxer: stream unregistered", slog.String("channel_id", channelID.String()))
	}

	stream := NewMuxerBidiStream(sendFn, cleanup)

	m.channelsMu.Lock()
	m.channels[channelID] = stream
	m.channelsMu.Unlock()

	slog.Debug("muxer: stream registered", slog.String("channel_id", channelID.String()))
	return stream
}

// readLoop continuously receives messages from the WebSocket,
// routes them to the appropriate stream, and auto-registers new streams.
func (m *WebSocketMuxer) readLoop() {

	for {
		var msg MuxerMsg
		if err := websocket.JSON.Receive(m.conn, &msg); err != nil {
			slog.Warn("muxer: websocket receive error", slog.String("error", err.Error()))
			return
		}

		m.channelsMu.RLock()
		stream, exists := m.channels[msg.ChannelID]
		m.channelsMu.RUnlock()

		if !exists {
			stream = m.register(msg.ChannelID)
			go m.node.Handle(m.Context, stream)
		}

		select {
		case stream.RecvChan() <- msg.Payload:
		case <-stream.closed:
			slog.Debug("muxer: dropped message for closed stream", slog.String("channel_id", msg.ChannelID.String()))
		}
	}
}
