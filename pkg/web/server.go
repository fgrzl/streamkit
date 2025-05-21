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
	router.GET("/streamkit", server.connect)
}

type webSocketServer struct {
	manager node.NodeManager
}

func (s *webSocketServer) connect(c *mux.RouteContext) {
	tenantIDClaim, ok := c.User.Claims()["tenant_id"]
	if !ok {
		c.Forbidden("missing tenant_id claim")
		return
	}
	tenantID, ok := tenantIDClaim.UUIDValue()
	if !ok {
		c.BadRequest("invalid tenant_id claim", "The tenant id must be an valid UUID")
		return
	}
	node, err := s.manager.GetOrCreate(c, tenantID)
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
