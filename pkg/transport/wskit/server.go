// Package wskit provides WebSocket-based transport implementation
// for the streamkit streaming platform.
//
// This package implements bidirectional streaming over WebSocket connections,
// including connection management, message multiplexing, and integration
// with the node management system.
package wskit

import (
	"context"

	"github.com/fgrzl/mux"
	"github.com/fgrzl/streamkit/pkg/node"
	"golang.org/x/net/websocket"
)

// ConfigureWebSocketServer configures a WebSocket endpoint on the provided router
// that integrates with the node manager for handling streaming requests.
func ConfigureWebSocketServer(router *mux.Router, manager node.NodeManager) {
	server := &webSocketServer{
		manager: manager,
	}
	router.GET("/", server.connect)
}

type webSocketServer struct {
	manager node.NodeManager
}

func (s *webSocketServer) connect(c mux.RouteContext) {

	session, err := NewServerMuxerSession(c.User())
	if err != nil {
		c.Unauthorized()
	}

	handler := &webSocketHandler{
		ctx:     c,
		session: session,
		manager: s.manager,
	}

	websocket.Handler(handler.handle).ServeHTTP(c.Response(), c.Request())
}

type webSocketHandler struct {
	ctx     context.Context
	session MuxerSession
	manager node.NodeManager
}

func (h *webSocketHandler) handle(conn *websocket.Conn) {
	NewServerWebSocketMuxer(h.ctx, h.session, h.manager, conn)
}
