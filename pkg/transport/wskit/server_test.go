package wskit

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fgrzl/claims"
	"github.com/fgrzl/claims/jwtkit"
	"github.com/fgrzl/mux"
	"github.com/fgrzl/streamkit/pkg/server"
	"github.com/fgrzl/streamkit/pkg/storage/pebblekit"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/websocket"
)

func newWebSocketTestServer(t *testing.T) (*httptest.Server, []byte) {
	t.Helper()

	secret := []byte("test-secret")
	validator := &jwtkit.HMAC256Validator{Secret: secret}
	factory, err := pebblekit.NewStoreFactory(&pebblekit.PebbleStoreOptions{Path: t.TempDir()})
	require.NoError(t, err)

	nodeManager := server.NewNodeManager(server.WithStoreFactory(factory))
	router := mux.NewRouter()
	mux.UseAuthentication(router, mux.WithAuthValidator(validator.Validate))
	mux.UseAuthorization(router)
	ConfigureWebSocketServer(router, nodeManager)

	ts := httptest.NewServer(router)
	t.Cleanup(func() {
		ts.Close()
		nodeManager.Close()
	})

	return ts, secret
}

func signWebSocketToken(t *testing.T, secret []byte, scope string) string {
	t.Helper()

	signer := jwtkit.HMAC256Signer{Secret: secret}
	token, err := signer.CreateToken(claims.NewPrincipalFromList(
		claims.NewClaimsList("tenant_id", uuid.NewString()).Add("scopes", scope),
	), time.Minute)
	require.NoError(t, err)
	return token
}

func TestShouldRejectMissingAuthenticationWhenConfigureWebSocketServer(t *testing.T) {
	ts, _ := newWebSocketTestServer(t)

	resp, err := ts.Client().Get(ts.URL + "/streamz")
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestShouldRejectPrincipalWithoutStreamkitScopeWhenConfigureWebSocketServer(t *testing.T) {
	ts, secret := newWebSocketTestServer(t)
	token := signWebSocketToken(t, secret, "other::*")

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/streamz", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	t.Cleanup(func() {
		resp.Body.Close()
	})

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestShouldAcceptAuthorizedWebSocketHandshakeWhenConfigureWebSocketServer(t *testing.T) {
	ts, secret := newWebSocketTestServer(t)
	token := signWebSocketToken(t, secret, ScopeAllStores)

	config, err := websocket.NewConfig("ws"+strings.TrimPrefix(ts.URL, "http")+"/streamz", "http://localhost")
	require.NoError(t, err)
	config.Header.Set("Authorization", "Bearer "+token)

	conn, err := websocket.DialConfig(config)
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.NoError(t, conn.Close())
}
