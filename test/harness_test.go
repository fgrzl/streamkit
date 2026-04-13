package test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/fgrzl/claims"
	"github.com/fgrzl/claims/jwtkit"
	"github.com/fgrzl/mux"
	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/fgrzl/streamkit/pkg/server"
	"github.com/fgrzl/streamkit/pkg/storage"
	"github.com/fgrzl/streamkit/pkg/storage/azurekit"
	"github.com/fgrzl/streamkit/pkg/storage/pebblekit"
	"github.com/fgrzl/streamkit/pkg/transport/inprockit"
	"github.com/fgrzl/streamkit/pkg/transport/wskit"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var secret = []byte("top-secret")
var tester = claims.NewClaimsList("tenant_id", uuid.NewString()).Add("scopes", "streamkit::*")

func wskitTestHarness(t *testing.T, factory storage.StoreFactory) *TestHarness {
	validator := &jwtkit.HMAC256Validator{
		Secret: secret,
	}

	nodeManager := server.NewNodeManager(server.WithStoreFactory(factory))
	router := mux.NewRouter()

	mux.UseAuthentication(router, mux.WithAuthValidator(validator.Validate))
	mux.UseAuthorization(router)
	router.Healthz().AllowAnonymous()
	wskit.ConfigureWebSocketServer(router, nodeManager)

	server := httptest.NewServer(router)
	t.Cleanup(func() {
		server.Close()
		nodeManager.Close()
	})

	httpClient := server.Client()
	resp, err := httpClient.Get(server.URL + "/healthz")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	signer := jwtkit.HMAC256Signer{Secret: secret}
	token, err := signer.CreateToken(claims.NewPrincipalFromList(tester), time.Minute)
	require.NoError(t, err)

	url, err := url.Parse(server.URL)
	require.NoError(t, err)

	addr := "ws://" + url.Host
	provider := wskit.NewBidiStreamProvider(addr, func() (string, error) { return token, nil })
	clientInstance := client.NewClient(provider)
	t.Cleanup(func() {
		clientInstance.Close()
	})

	return &TestHarness{Client: clientInstance}
}

func azurekitTestHarness(t *testing.T) *TestHarness {
	options := &azurekit.AzureStoreOptions{
		Prefix:            uuid.NewString(),
		AccountName:       "devstoreaccount1",
		AccountKey:        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
		Endpoint:          "http://127.0.0.1:10002/devstoreaccount1",
		AllowInsecureHTTP: true,
	}

	factory, err := azurekit.NewStoreFactory(t.Context(), options)
	require.NoError(t, err, "azure store factory required for azure configuration")

	return wskitTestHarness(t, factory)
}

func pebblekitTestHarness(t *testing.T) *TestHarness {
	options := &pebblekit.PebbleStoreOptions{Path: t.TempDir()}
	factory, err := pebblekit.NewStoreFactory(options)
	require.NoError(t, err)

	return wskitTestHarness(t, factory)
}

func inprockitTestHarness(t *testing.T) *TestHarness {
	options := &pebblekit.PebbleStoreOptions{Path: t.TempDir()}
	factory, err := pebblekit.NewStoreFactory(options)
	require.NoError(t, err)

	nodeManager := server.NewNodeManager(server.WithStoreFactory(factory))
	provider := inprockit.NewInProcBidiStreamProvider(t.Context(), nodeManager)
	clientInstance := client.NewClient(provider)

	t.Cleanup(func() {
		clientInstance.Close()
		nodeManager.Close()
	})

	return &TestHarness{Client: clientInstance}
}

func configurations() map[string]func(*testing.T) *TestHarness {
	return map[string]func(*testing.T) *TestHarness{
		"azure":  azurekitTestHarness,
		"pebble": pebblekitTestHarness,
		"inproc": inprockitTestHarness,
	}
}
