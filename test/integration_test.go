package test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/fgrzl/mux"
	"github.com/fgrzl/streamkit"
	"github.com/fgrzl/streamkit/pkg/auth/jwtkit"
	"github.com/fgrzl/streamkit/pkg/transport/wskit"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/websocket"
)

func SetupTestServer(t *testing.T) *httptest.Server {
	router := mux.NewRouter()
	router.Healthz()
	router.GET("/ws", func(c *mux.RouteContext) {
		websocket.Handler(func(ws *websocket.Conn) {
			wskit.NewWebSocketMuxer(ws)
		}).ServeHTTP(c.Response, c.Request)
	})
	server := httptest.NewServer(router)
	t.Cleanup(func() {
		server.Close()
	})

	client := server.Client()

	resp, err := client.Get(server.URL + "/healthz")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	return server
}

func TestConnection(t *testing.T) {
	ctx := context.Background()
	server := SetupTestServer(t)

	signer := jwtkit.Signer{}
	token, err := signer.CreateToken("test", time.Minute*1)
	require.NoError(t, err)

	provider := wskit.NewBidiStreamProvider(server.URL+"/ws", token)
	client := streamkit.NewClient(provider)

	entry, err := client.Peek(ctx, "test", "test")
	require.NoError(t, err)
	require.Equal(t, &streamkit.Entry{}, entry)
}
