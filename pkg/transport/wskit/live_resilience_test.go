package wskit

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgrzl/claims/jwtkit"
	"github.com/fgrzl/mux"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/bus"
	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/fgrzl/streamkit/pkg/server"
	"github.com/fgrzl/streamkit/pkg/storage/pebblekit"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/websocket"
)

type liveWebSocketServer struct {
	server    *httptest.Server
	manager   server.NodeManager
	bus       *liveMessageBus
	closeOnce sync.Once
}

type liveMessageBus struct {
	mu             sync.RWMutex
	subscribers    map[string]map[uint64]bus.MessageHandler
	nextID         atomic.Uint64
	subscribeCalls atomic.Int32
}

type liveMessageBusSubscription struct {
	bus      *liveMessageBus
	routeKey string
	id       uint64
	once     sync.Once
}

type liveMessageBusFactory struct {
	bus bus.MessageBus
}

func (f *liveMessageBusFactory) Get(context.Context) (bus.MessageBus, error) {
	return f.bus, nil
}

func newLiveMessageBus() *liveMessageBus {
	return &liveMessageBus{
		subscribers: make(map[string]map[uint64]bus.MessageHandler),
	}
}

func (b *liveMessageBus) Notify(msg bus.Message) error {
	return b.NotifyWithContext(context.Background(), msg)
}

func (b *liveMessageBus) NotifyWithContext(ctx context.Context, msg bus.Message) error {
	if msg == nil {
		return nil
	}

	routeKey := msg.GetRoute().String()
	b.mu.RLock()
	routeSubscribers := b.subscribers[routeKey]
	handlers := make([]bus.MessageHandler, 0, len(routeSubscribers))
	for _, handler := range routeSubscribers {
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

func (b *liveMessageBus) Subscribe(route bus.Route, handler bus.MessageHandler) (bus.Subscription, error) {
	routeKey := route.String()
	id := b.nextID.Add(1)
	b.subscribeCalls.Add(1)

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subscribers[routeKey] == nil {
		b.subscribers[routeKey] = make(map[uint64]bus.MessageHandler)
	}
	b.subscribers[routeKey][id] = handler
	return &liveMessageBusSubscription{
		bus:      b,
		routeKey: routeKey,
		id:       id,
	}, nil
}

func (b *liveMessageBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subscribers = make(map[string]map[uint64]bus.MessageHandler)
	return nil
}

func (b *liveMessageBus) subscriberCount(route bus.Route) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscribers[route.String()])
}

func (b *liveMessageBus) totalSubscribeCalls() int32 {
	return b.subscribeCalls.Load()
}

func (s *liveMessageBusSubscription) Unsubscribe() error {
	s.once.Do(func() {
		s.bus.mu.Lock()
		defer s.bus.mu.Unlock()
		routeSubscribers := s.bus.subscribers[s.routeKey]
		delete(routeSubscribers, s.id)
		if len(routeSubscribers) == 0 {
			delete(s.bus.subscribers, s.routeKey)
		}
	})
	return nil
}

func newLivePebbleWebSocketServer(t *testing.T, path string, validator *jwtkit.HMAC256Validator) *liveWebSocketServer {
	t.Helper()
	if path == "" {
		path = t.TempDir()
	}

	factory, err := pebblekit.NewStoreFactory(&pebblekit.PebbleStoreOptions{Path: path})
	require.NoError(t, err)

	messageBus := newLiveMessageBus()
	nodeManager := server.NewNodeManager(
		server.WithStoreFactory(factory),
		server.WithMessageBusFactory(&liveMessageBusFactory{bus: messageBus}),
	)
	router := mux.NewRouter()
	mux.UseAuthentication(router, mux.WithAuthValidator(validator.Validate))
	mux.UseAuthorization(router)
	router.Healthz().AllowAnonymous()
	ConfigureWebSocketServer(router, nodeManager)

	s := &liveWebSocketServer{
		server:  httptest.NewServer(router),
		manager: nodeManager,
		bus:     messageBus,
	}
	t.Cleanup(s.Close)
	return s
}

func (s *liveWebSocketServer) Close() {
	s.closeOnce.Do(func() {
		if s.server != nil {
			s.server.Close()
		}
		if s.manager != nil {
			s.manager.Close()
		}
		if s.bus != nil {
			_ = s.bus.Close()
		}
	})
}

func (s *liveWebSocketServer) URL() string {
	if s == nil || s.server == nil {
		return ""
	}
	return s.server.URL
}

func liveRetryPolicy() client.RetryPolicy {
	return client.RetryPolicy{
		MaxAttempts:       6,
		InitialBackoff:    50 * time.Millisecond,
		MaxBackoff:        500 * time.Millisecond,
		BackoffMultiplier: 2,
	}
}

func dialAuthorizedWebSocket(baseURL, token string) (*websocket.Conn, error) {
	cfg, err := websocket.NewConfig("ws"+strings.TrimPrefix(baseURL, "http")+"/streamz", "http://localhost")
	if err != nil {
		return nil, err
	}
	if cfg.Header == nil {
		cfg.Header = http.Header{}
	}
	cfg.Header.Set("Authorization", "Bearer "+token)
	return websocket.DialConfig(cfg)
}

func webSocketBaseURL(baseURL string) string {
	return "ws" + strings.TrimPrefix(baseURL, "http")
}

func closeCurrentConn(t *testing.T, p *WebSocketBidiStreamProvider) {
	t.Helper()

	var conn *websocket.Conn
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		muxer, ok := p.muxer.(*WebSocketMuxer)
		if !ok || muxer == nil || muxer.conn == nil {
			return false
		}
		conn = muxer.conn
		return true
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, conn.Close())
}

func requireEventuallyPeekSequence(t *testing.T, c client.Client, storeID uuid.UUID, space, segment string, expected uint64) {
	t.Helper()

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		entry, err := c.Peek(ctx, storeID, space, segment)
		return err == nil && entry != nil && entry.Sequence == expected
	}, 10*time.Second, 100*time.Millisecond)
}

func newLiveClientWithRetry(t *testing.T, baseURL string, fetchJWT func() (string, error)) client.Client {
	t.Helper()

	provider := NewBidiStreamProvider(webSocketBaseURL(baseURL), fetchJWT).(*WebSocketBidiStreamProvider)
	clientInstance := client.NewClientWithRetryPolicy(provider, liveRetryPolicy())
	t.Cleanup(func() {
		assert.NoError(t, clientInstance.Close())
		assert.NoError(t, provider.Close())
	})
	return clientInstance
}

func waitForSubscriptionSequenceAtLeast(t *testing.T, updates <-chan client.SegmentStatus, minSequence uint64, timeout time.Duration) client.SegmentStatus {
	t.Helper()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case status := <-updates:
			if status.LastSequence >= minSequence {
				return status
			}
		case <-deadline.C:
			t.Fatalf("timeout waiting for subscription sequence >= %d", minSequence)
		}
	}
}

func requireEventuallySubscriptionSequenceAtLeast(t *testing.T, c client.Client, storeID uuid.UUID, space, segment string, updates <-chan client.SegmentStatus, minSequence uint64) {
	t.Helper()

	var lastSeen uint64
	publishAttempt := 0
	require.Eventually(t, func() bool {
		for {
			select {
			case status := <-updates:
				if status.LastSequence > lastSeen {
					lastSeen = status.LastSequence
				}
			default:
				goto drained
			}
		}

	drained:
		if lastSeen >= minSequence {
			return true
		}

		publishAttempt++
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := c.Publish(ctx, storeID, space, segment, []byte(fmt.Sprintf("subscription-replay-%d", publishAttempt)), nil)
		cancel()
		return err == nil && lastSeen >= minSequence
	}, 10*time.Second, 100*time.Millisecond)
}

func waitForLiveSubscriptionRegistration(t *testing.T, bus *liveMessageBus, storeID uuid.UUID, space string, minSubscribeCalls int32) {
	t.Helper()
	route := api.GetSegmentNotificationRoute(storeID, space)
	require.Eventually(t, func() bool {
		if bus == nil {
			return false
		}
		return bus.totalSubscribeCalls() >= minSubscribeCalls && bus.subscriberCount(route) > 0
	}, 10*time.Second, 25*time.Millisecond)
}

func TestLiveClientRecoversAfterWebSocketDrop(t *testing.T) {
	secret := []byte("drop-secret")
	server := newLivePebbleWebSocketServer(t, t.TempDir(), &jwtkit.HMAC256Validator{Secret: secret})
	token := signWebSocketToken(t, secret, ScopeAllStores)

	provider := NewBidiStreamProvider(webSocketBaseURL(server.URL()), func() (string, error) {
		return token, nil
	}).(*WebSocketBidiStreamProvider)
	listener := &testReconnectListener{}
	provider.RegisterReconnectListener(listener)

	clientInstance := client.NewClientWithRetryPolicy(provider, liveRetryPolicy())
	t.Cleanup(func() {
		assert.NoError(t, clientInstance.Close())
		assert.NoError(t, provider.Close())
	})

	storeID := uuid.New()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, clientInstance.Publish(ctx, storeID, "space", "segment", []byte("first"), nil))

	closeCurrentConn(t, provider)

	requireEventuallyPeekSequence(t, clientInstance, storeID, "space", "segment", 1)
	require.Eventually(t, func() bool {
		return listener.count() >= 1
	}, 5*time.Second, 50*time.Millisecond)
}

func TestLiveClientRecoversAfterServerRestart(t *testing.T) {
	path := t.TempDir()
	secret := []byte("restart-secret")
	server := newLivePebbleWebSocketServer(t, path, &jwtkit.HMAC256Validator{Secret: secret})
	token := signWebSocketToken(t, secret, ScopeAllStores)

	var currentBaseURL atomic.Value
	currentBaseURL.Store(server.URL())

	provider := NewBidiStreamProvider(webSocketBaseURL(server.URL()), func() (string, error) {
		return token, nil
	}).(*WebSocketBidiStreamProvider)
	provider.dialFn = func() (*websocket.Conn, error) {
		return dialAuthorizedWebSocket(currentBaseURL.Load().(string), token)
	}
	listener := &testReconnectListener{}
	provider.RegisterReconnectListener(listener)

	clientInstance := client.NewClientWithRetryPolicy(provider, liveRetryPolicy())
	t.Cleanup(func() {
		assert.NoError(t, clientInstance.Close())
		assert.NoError(t, provider.Close())
	})

	storeID := uuid.New()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, clientInstance.Publish(ctx, storeID, "space", "segment", []byte("first"), nil))

	closeCurrentConn(t, provider)
	server.Close()

	restarted := newLivePebbleWebSocketServer(t, path, &jwtkit.HMAC256Validator{Secret: secret})
	currentBaseURL.Store(restarted.URL())

	requireEventuallyPeekSequence(t, clientInstance, storeID, "space", "segment", 1)
	require.Eventually(t, func() bool {
		return listener.count() >= 1
	}, 10*time.Second, 50*time.Millisecond)
}

func TestLiveClientRefreshesTokenAfterAuthFailureOnReconnect(t *testing.T) {
	secretA := []byte("rotate-secret-a")
	secretB := []byte("rotate-secret-b")
	validator := &jwtkit.HMAC256Validator{Secret: secretA}
	server := newLivePebbleWebSocketServer(t, t.TempDir(), validator)

	tokenA := signWebSocketToken(t, secretA, ScopeAllStores)
	tokenB := signWebSocketToken(t, secretB, ScopeAllStores)

	var currentToken atomic.Value
	currentToken.Store(tokenA)

	provider := NewBidiStreamProvider(webSocketBaseURL(server.URL()), func() (string, error) {
		return currentToken.Load().(string), nil
	}).(*WebSocketBidiStreamProvider)
	provider.RetryAuthFailures = true

	var dialFailures atomic.Int32
	provider.OnDialFailure = func(err error) {
		if err != nil {
			dialFailures.Add(1)
			currentToken.Store(tokenB)
		}
	}

	listener := &testReconnectListener{}
	provider.RegisterReconnectListener(listener)

	clientInstance := client.NewClientWithRetryPolicy(provider, liveRetryPolicy())
	t.Cleanup(func() {
		assert.NoError(t, clientInstance.Close())
		assert.NoError(t, provider.Close())
	})

	storeID := uuid.New()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, clientInstance.Publish(ctx, storeID, "space", "segment", []byte("first"), nil))

	validator.Secret = secretB
	closeCurrentConn(t, provider)

	requireEventuallyPeekSequence(t, clientInstance, storeID, "space", "segment", 1)
	require.Eventually(t, func() bool {
		return dialFailures.Load() >= 1
	}, 10*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool {
		return listener.count() >= 1
	}, 10*time.Second, 50*time.Millisecond)
}

func TestLiveSubscriptionRefreshesTokenAfterAuthFailureOnReconnect(t *testing.T) {
	secretA := []byte("subscription-rotate-secret-a")
	secretB := []byte("subscription-rotate-secret-b")
	validator := &jwtkit.HMAC256Validator{Secret: secretA}
	server := newLivePebbleWebSocketServer(t, t.TempDir(), validator)

	tokenA := signWebSocketToken(t, secretA, ScopeAllStores)
	tokenB := signWebSocketToken(t, secretB, ScopeAllStores)

	var currentToken atomic.Value
	currentToken.Store(tokenA)

	provider := NewBidiStreamProvider(webSocketBaseURL(server.URL()), func() (string, error) {
		return currentToken.Load().(string), nil
	}).(*WebSocketBidiStreamProvider)
	provider.RetryAuthFailures = true

	var dialFailures atomic.Int32
	provider.OnDialFailure = func(err error) {
		if err != nil {
			dialFailures.Add(1)
			currentToken.Store(tokenB)
		}
	}

	listener := &testReconnectListener{}
	provider.RegisterReconnectListener(listener)

	clientInstance := client.NewClientWithRetryPolicy(provider, liveRetryPolicy())
	t.Cleanup(func() {
		assert.NoError(t, clientInstance.Close())
		assert.NoError(t, provider.Close())
	})

	storeID := uuid.New()
	updates := make(chan client.SegmentStatus, 32)
	sub, err := clientInstance.SubscribeToSegment(context.Background(), storeID, "space", "segment", func(status *client.SegmentStatus) {
		select {
		case updates <- *status:
		default:
		}
	})
	require.NoError(t, err)
	t.Cleanup(sub.Unsubscribe)
	waitForLiveSubscriptionRegistration(t, server.bus, storeID, "space", 1)

	writerClient := newLiveClientWithRetry(t, server.URL(), func() (string, error) {
		return tokenA, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, writerClient.Publish(ctx, storeID, "space", "segment", []byte("first"), nil))
	first := waitForSubscriptionSequenceAtLeast(t, updates, 1, 10*time.Second)
	assert.Equal(t, uint64(1), first.LastSequence)

	allowReconnect := make(chan struct{})
	provider.dialFn = func() (*websocket.Conn, error) {
		<-allowReconnect
		return dialAuthorizedWebSocket(server.URL(), currentToken.Load().(string))
	}

	validator.Secret = secretB
	closeCurrentConn(t, provider)

	writerAfterRotate := newLiveClientWithRetry(t, server.URL(), func() (string, error) {
		return tokenB, nil
	})
	publishCtx, publishCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer publishCancel()
	require.NoError(t, writerAfterRotate.Publish(publishCtx, storeID, "space", "segment", []byte("second"), nil))

	close(allowReconnect)

	require.Eventually(t, func() bool {
		return dialFailures.Load() >= 1
	}, 10*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool {
		return listener.count() >= 1
	}, 10*time.Second, 50*time.Millisecond)
	waitForLiveSubscriptionRegistration(t, server.bus, storeID, "space", 2)

	second := waitForSubscriptionSequenceAtLeast(t, updates, 2, 10*time.Second)
	assert.Equal(t, uint64(2), second.LastSequence)
}

func TestLiveSubscriptionRecoversAfterWebSocketDrop(t *testing.T) {
	secret := []byte("subscription-drop-secret")
	server := newLivePebbleWebSocketServer(t, t.TempDir(), &jwtkit.HMAC256Validator{Secret: secret})
	token := signWebSocketToken(t, secret, ScopeAllStores)

	provider := NewBidiStreamProvider(webSocketBaseURL(server.URL()), func() (string, error) {
		return token, nil
	}).(*WebSocketBidiStreamProvider)
	listener := &testReconnectListener{}
	provider.RegisterReconnectListener(listener)

	clientInstance := client.NewClientWithRetryPolicy(provider, liveRetryPolicy())
	t.Cleanup(func() {
		assert.NoError(t, clientInstance.Close())
		assert.NoError(t, provider.Close())
	})

	storeID := uuid.New()
	updates := make(chan client.SegmentStatus, 32)
	sub, err := clientInstance.SubscribeToSegment(context.Background(), storeID, "space", "segment", func(status *client.SegmentStatus) {
		select {
		case updates <- *status:
		default:
		}
	})
	require.NoError(t, err)
	t.Cleanup(sub.Unsubscribe)
	waitForLiveSubscriptionRegistration(t, server.bus, storeID, "space", 1)

	writerClient := newLiveClientWithRetry(t, server.URL(), func() (string, error) {
		return token, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, writerClient.Publish(ctx, storeID, "space", "segment", []byte("first"), nil))
	first := waitForSubscriptionSequenceAtLeast(t, updates, 1, 10*time.Second)
	assert.Equal(t, uint64(1), first.LastSequence)

	allowReconnect := make(chan struct{})
	provider.dialFn = func() (*websocket.Conn, error) {
		<-allowReconnect
		return dialAuthorizedWebSocket(server.URL(), token)
	}

	closeCurrentConn(t, provider)
	publishCtx, publishCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer publishCancel()
	require.NoError(t, writerClient.Publish(publishCtx, storeID, "space", "segment", []byte("second"), nil))
	close(allowReconnect)
	require.Eventually(t, func() bool {
		return listener.count() >= 1
	}, 10*time.Second, 50*time.Millisecond)
	waitForLiveSubscriptionRegistration(t, server.bus, storeID, "space", 2)

	second := waitForSubscriptionSequenceAtLeast(t, updates, 2, 10*time.Second)
	assert.Equal(t, uint64(2), second.LastSequence)
}

func TestLiveSubscriptionRecoversAfterServerRestart(t *testing.T) {
	path := t.TempDir()
	secret := []byte("subscription-restart-secret")
	server := newLivePebbleWebSocketServer(t, path, &jwtkit.HMAC256Validator{Secret: secret})
	token := signWebSocketToken(t, secret, ScopeAllStores)

	var currentBaseURL atomic.Value
	currentBaseURL.Store(server.URL())

	provider := NewBidiStreamProvider(webSocketBaseURL(server.URL()), func() (string, error) {
		return token, nil
	}).(*WebSocketBidiStreamProvider)
	provider.dialFn = func() (*websocket.Conn, error) {
		return dialAuthorizedWebSocket(currentBaseURL.Load().(string), token)
	}
	listener := &testReconnectListener{}
	provider.RegisterReconnectListener(listener)

	clientInstance := client.NewClientWithRetryPolicy(provider, liveRetryPolicy())
	t.Cleanup(func() {
		assert.NoError(t, clientInstance.Close())
		assert.NoError(t, provider.Close())
	})

	storeID := uuid.New()
	updates := make(chan client.SegmentStatus, 32)
	sub, err := clientInstance.SubscribeToSegment(context.Background(), storeID, "space", "segment", func(status *client.SegmentStatus) {
		select {
		case updates <- *status:
		default:
		}
	})
	require.NoError(t, err)
	t.Cleanup(sub.Unsubscribe)
	waitForLiveSubscriptionRegistration(t, server.bus, storeID, "space", 1)

	writerClient := newLiveClientWithRetry(t, server.URL(), func() (string, error) {
		return token, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, writerClient.Publish(ctx, storeID, "space", "segment", []byte("first"), nil))
	first := waitForSubscriptionSequenceAtLeast(t, updates, 1, 10*time.Second)
	assert.Equal(t, uint64(1), first.LastSequence)

	allowReconnect := make(chan struct{})
	provider.dialFn = func() (*websocket.Conn, error) {
		<-allowReconnect
		return dialAuthorizedWebSocket(currentBaseURL.Load().(string), token)
	}

	closeCurrentConn(t, provider)
	server.Close()

	restarted := newLivePebbleWebSocketServer(t, path, &jwtkit.HMAC256Validator{Secret: secret})
	currentBaseURL.Store(restarted.URL())
	writerAfterRestart := newLiveClientWithRetry(t, restarted.URL(), func() (string, error) {
		return token, nil
	})
	publishCtx, publishCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer publishCancel()
	require.NoError(t, writerAfterRestart.Publish(publishCtx, storeID, "space", "segment", []byte("second"), nil))
	close(allowReconnect)
	require.Eventually(t, func() bool {
		return listener.count() >= 1
	}, 10*time.Second, 50*time.Millisecond)
	waitForLiveSubscriptionRegistration(t, restarted.bus, storeID, "space", 1)

	second := waitForSubscriptionSequenceAtLeast(t, updates, 2, 10*time.Second)
	assert.Equal(t, uint64(2), second.LastSequence)
}
