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
	name       string
	conn       *websocket.Conn
	channels   map[uuid.UUID]*MuxerBidiStream
	channelsMu sync.RWMutex
	writeMu    sync.Mutex
	done       chan struct{}
	node       node.Node
}

// NewClientWebSocketMuxer will spawn a read loop as a go routine and returns the *WebSocketMuxer
func NewClientWebSocketMuxer(ctx context.Context, conn *websocket.Conn) *WebSocketMuxer {
	m := &WebSocketMuxer{
		Context:  ctx,
		name:     "client",
		conn:     conn,
		channels: make(map[uuid.UUID]*MuxerBidiStream),
		done:     make(chan struct{}),
	}
	go m.readLoop()
	return m
}

// NewServerWebSocketMuxer will start a blocking read loop to keep the websocket connection open
func NewServerWebSocketMuxer(ctx context.Context, node node.Node, conn *websocket.Conn) {
	m := &WebSocketMuxer{
		Context:  ctx,
		name:     "server",
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

	bidi := NewMuxerBidiStream(sendFn, cleanup)

	m.channelsMu.Lock()
	m.channels[channelID] = bidi
	m.channelsMu.Unlock()

	slog.Debug("muxer: stream registered", slog.String("channel_id", channelID.String()))
	return bidi
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
		bidi, exists := m.channels[msg.ChannelID]
		m.channelsMu.RUnlock()

		ctx := node.WithChannelID(m.Context, msg.ChannelID)

		if !exists {
			if m.node == nil {
				// Client side the channel is registered before the read loop starts
				// If this is a server side err then the node should not be nil
				slog.ErrorContext(ctx, "node does not exists")
			}
			bidi = m.register(msg.ChannelID)

			go m.node.Handle(ctx, bidi)
		}

		select {
		case bidi.RecvChan() <- msg.Payload:
			slog.DebugContext(ctx, "muxer: sent message", slog.String("channel_id", msg.ChannelID.String()))
		case <-bidi.closed:
			slog.DebugContext(ctx, "muxer: dropped message for closed stream", slog.String("channel_id", msg.ChannelID.String()))
		}
	}
}
