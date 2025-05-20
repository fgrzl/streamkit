package web

import (
	"context"

	"github.com/fgrzl/mux"
	"github.com/fgrzl/streamkit/pkg/node"
	"github.com/fgrzl/streamkit/pkg/transport/wskit"
	"golang.org/x/net/websocket"
)

func ConfigureWebSocketServer(router *mux.Router, manager node.NodeManager) {
	server := &webSocketServer{
		manager: manager,
	}
	router.GET("/ws", server.connect)
}

type webSocketServer struct {
	manager node.NodeManager
}

func (s *webSocketServer) connect(c *mux.RouteContext) {
	tenantID, ok := c.User.Claims()["tenant_id"]
	if !ok {
		c.Forbidden("missing tenant")
		return
	}
	node, err := s.manager.GetOrCreate(c, tenantID.Value())
	if err != nil {
		c.ServerError("Could not connect", err.Error())
		return
	}

	handler := &webSocketHandler{
		ctx:  c,
		node: node,
	}

	websocket.Handler(handler.handle).ServeHTTP(c.Response, c.Request)
}

type webSocketHandler struct {
	ctx  context.Context
	node node.Node
}

func (h *webSocketHandler) handle(ws *websocket.Conn) {
	wskit.NewServerWebSocketMuxer(h.ctx, h.node, ws)
}
