package test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/fgrzl/claims"
	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/mux"
	"github.com/fgrzl/streamkit"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/auth/jwtkit"
	"github.com/fgrzl/streamkit/pkg/storage"
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

		// at this point we should have the claims.Principal
		// get the tenant_id from the claims principal
		// get or create a service node

		tenantID, ok := c.User.Claims()["tenant_id"]
		if(!ok){
			c.Forbidden("missing tenant")
			return
		}
		provider := 

		var store storage.Store

		websocket.Handler(func(ws *websocket.Conn) {
			wskit.NewServerWebSocketMuxer(ws, func(bidiStream api.BidiStream) {

				envelope := &polymorphic.Envelope{}
				if err := bidiStream.Decode(envelope); err != nil {
					bidiStream.Close(err)
				}

				// get the node for this tenant
				switch v := envelope.Content.(type) {
				case *api.Peek:
					store.Peek(c, v.Space, v.Segment)

				default:
					bidiStream.Close(fmt.Errorf("invalid request msg"))
				}

			})
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
