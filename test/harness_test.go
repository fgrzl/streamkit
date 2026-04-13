package test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/fgrzl/claims"
	"github.com/fgrzl/claims/jwtkit"
	"github.com/fgrzl/mux"
	"github.com/fgrzl/streamkit/pkg/bus"
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

type testMessageBus struct {
	mu       sync.RWMutex
	handlers map[string]map[uint64]bus.MessageHandler
	nextID   uint64
}

type testMessageBusSub struct {
	b    *testMessageBus
	key  string
	id   uint64
	once sync.Once
}

type testMessageBusFactory struct {
	bus bus.MessageBus
}

func (f *testMessageBusFactory) Get(context.Context) (bus.MessageBus, error) {
	return f.bus, nil
}

func newTestMessageBus() *testMessageBus {
	return &testMessageBus{handlers: make(map[string]map[uint64]bus.MessageHandler)}
}

func (b *testMessageBus) Notify(msg bus.Message) error {
	return b.NotifyWithContext(context.Background(), msg)
}

func (b *testMessageBus) NotifyWithContext(ctx context.Context, msg bus.Message) error {
	if msg == nil {
		return nil
	}

	key := msg.GetRoute().String()
	b.mu.RLock()
	routeHandlers := b.handlers[key]
	handlers := make([]bus.MessageHandler, 0, len(routeHandlers))
	for _, handler := range routeHandlers {
		handlers = append(handlers, handler)
	}
	b.mu.RUnlock()

	for _, handler := range handlers {
		if err := handler(ctx, msg); err != nil {
			return err
		}
	}

	return nil
}

func (b *testMessageBus) Subscribe(route bus.Route, handler bus.MessageHandler) (bus.Subscription, error) {
	key := route.String()

	b.mu.Lock()
	defer b.mu.Unlock()

	b.nextID++
	id := b.nextID
	if b.handlers[key] == nil {
		b.handlers[key] = make(map[uint64]bus.MessageHandler)
	}
	b.handlers[key][id] = handler

	return &testMessageBusSub{b: b, key: key, id: id}, nil
}

func (b *testMessageBus) Close() error {
	b.mu.Lock()
	b.handlers = make(map[string]map[uint64]bus.MessageHandler)
	b.mu.Unlock()
	return nil
}

func (s *testMessageBusSub) Unsubscribe() error {
	s.once.Do(func() {
		s.b.mu.Lock()
		if routeHandlers := s.b.handlers[s.key]; routeHandlers != nil {
			delete(routeHandlers, s.id)
			if len(routeHandlers) == 0 {
				delete(s.b.handlers, s.key)
			}
		}
		s.b.mu.Unlock()
	})
	return nil
}

func wskitTestHarness(t *testing.T, factory storage.StoreFactory) *TestHarness {
	validator := &jwtkit.HMAC256Validator{
		Secret: secret,
	}

	messageBus := newTestMessageBus()
	nodeManager := server.NewNodeManager(
		server.WithStoreFactory(factory),
		server.WithMessageBusFactory(&testMessageBusFactory{bus: messageBus}),
	)
	router := mux.NewRouter()

	mux.UseAuthentication(router, mux.WithAuthValidator(validator.Validate))
	mux.UseAuthorization(router)
	router.Healthz().AllowAnonymous()
	wskit.ConfigureWebSocketServer(router, nodeManager)

	server := httptest.NewServer(router)
	t.Cleanup(func() {
		server.Close()
		_ = messageBus.Close()
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

	messageBus := newTestMessageBus()
	nodeManager := server.NewNodeManager(
		server.WithStoreFactory(factory),
		server.WithMessageBusFactory(&testMessageBusFactory{bus: messageBus}),
	)
	provider := inprockit.NewInProcBidiStreamProvider(t.Context(), nodeManager)
	clientInstance := client.NewClient(provider)

	t.Cleanup(func() {
		clientInstance.Close()
		_ = messageBus.Close()
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
