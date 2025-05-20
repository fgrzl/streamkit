package test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/fgrzl/claims"
	"github.com/fgrzl/mux"
	"github.com/fgrzl/streamkit"
	"github.com/fgrzl/streamkit/pkg/auth/jwtkit"
	"github.com/fgrzl/streamkit/pkg/node"
	"github.com/fgrzl/streamkit/pkg/storage/azure"
	"github.com/fgrzl/streamkit/pkg/transport/wskit"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/websocket"
)

var secret = []byte("top-secret")
var tester = jwt.MapClaims{
	"tenant_id": "test",
}

func SetupTestServer(t *testing.T) *httptest.Server {

	validator := &jwtkit.HMAC256Validator{
		Secret: secret,
	}

	// Default Azurite configuration for local testing
	accountName := "devstoreaccount1"
	accountKey := "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	endpoint := "http://127.0.0.1:10002/devstoreaccount1"

	credential, err := azure.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		panic(err)
	}

	options := &azure.AzureStoreOptions{
		Prefix:              "test",
		Endpoint:            endpoint,
		SharedKeyCredential: credential,
		AllowInsecureHTTP:   true,
	}

	factory, err := azure.NewStoreFactory(options)
	require.NoError(t, err)

	nodeManager := node.NewNodeManager(factory)

	router := mux.NewRouter()

	router.UseAuthentication(&mux.AuthenticationOptions{
		Validate: func(token string) (claims.Principal, bool) {
			claimsMap, err := validator.Validate(token)
			if err != nil {
				return nil, false
			}

			return jwtkit.NewClaimsPrincipal(claimsMap), true
		},
	})

	router.UseAuthorization(&mux.AuthorizationOptions{})

	router.Healthz().AllowAnonymous()

	router.GET("/ws", func(c *mux.RouteContext) {
		tenantID, ok := c.User.Claims()["tenant_id"]
		if !ok {
			c.Forbidden("missing tenant")
			return
		}
		node, err := nodeManager.GetOrCreate(c, tenantID.Value())
		require.NoError(t, err)
		websocket.Handler(func(ws *websocket.Conn) {
			wskit.NewServerWebSocketMuxer(ws, node)
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

	signer := jwtkit.HMAC256Signer{
		Secret: secret,
	}
	token, err := signer.CreateToken(tester, time.Minute*1)
	require.NoError(t, err)

	url, err := url.Parse(server.URL)
	require.NoError(t, err)

	addr := "ws://" + url.Host + "/ws"
	provider := wskit.NewBidiStreamProvider(addr, token)
	client := streamkit.NewClient(provider)

	entry, err := client.Peek(ctx, "test", "test")
	require.NoError(t, err)
	require.Equal(t, &streamkit.Entry{}, entry)
}
