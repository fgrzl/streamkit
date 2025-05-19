package wskit

import (
	"log/slog"
	"sync"

	"github.com/fgrzl/streams/broker"
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
	conn       *websocket.Conn
	channels   map[uuid.UUID]*MuxerBidiStream
	channelsMu sync.RWMutex
	writeMu    sync.Mutex
}

// NewWebSocketMuxer wraps a websocket.Conn and starts its internal reader loop.
func NewWebSocketMuxer(conn *websocket.Conn) *WebSocketMuxer {
	m := &WebSocketMuxer{
		conn:     conn,
		channels: make(map[uuid.UUID]*MuxerBidiStream),
	}
	go m.readLoop()
	return m
}

// Register creates and tracks a new stream for the given ChannelID.
// If a stream with this ID already exists, it is overwritten.
func (m *WebSocketMuxer) Register(channelID uuid.UUID) broker.BidiStream {
	return m.register(channelID)
}

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
// routes them to the appropriate stream, and auto-registers new streams on first message.
func (m *WebSocketMuxer) readLoop() {
	for {
		var msg MuxerMsg
		if err := websocket.JSON.Receive(m.conn, &msg); err != nil {
			slog.Warn("muxer: websocket receive error", slog.String("error", err.Error()))
			return
		}

		m.channelsMu.RLock()
		stream, ok := m.channels[msg.ChannelID]
		m.channelsMu.RUnlock()

		if !ok {
			stream = m.register(msg.ChannelID)
			slog.Debug("muxer: auto-registered stream on first message", slog.String("channel_id", msg.ChannelID.String()))
		}

		select {
		case stream.RecvChan() <- msg.Payload:
		case <-stream.closed:
			slog.Debug("muxer: dropped message for closed stream", slog.String("channel_id", msg.ChannelID.String()))
		}
	}
}
