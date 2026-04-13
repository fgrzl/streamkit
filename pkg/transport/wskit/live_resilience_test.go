package wskit

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgrzl/claims/jwtkit"
	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/lexkey"
	"github.com/fgrzl/mux"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/bus"
	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/fgrzl/streamkit/pkg/server"
	"github.com/fgrzl/streamkit/pkg/storage"
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

type liveConsumeStoreFactory struct {
	store *liveConsumeStore
}

type liveConsumeStore struct {
	entries        []*api.Entry
	gateAfterIndex int
	gateMu         sync.Mutex
	gateAvailable  bool
	gateStarted    chan struct{}
	releaseGate    chan struct{}
}

type liveConsumeGate struct {
	started         chan struct{}
	release         chan struct{}
	blockAfterIndex int
	once            sync.Once
}

type liveConsumeEnumerator struct {
	ctx   context.Context
	items []*api.Entry
	idx   int
	gate  *liveConsumeGate
}

func (f *liveMessageBusFactory) Get(context.Context) (bus.MessageBus, error) {
	return f.bus, nil
}

func (f *liveConsumeStoreFactory) NewStore(context.Context, uuid.UUID) (storage.Store, error) {
	return f.store, nil
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

func newLiveWebSocketServerWithStoreFactory(t *testing.T, storeFactory storage.StoreFactory, validator *jwtkit.HMAC256Validator) *liveWebSocketServer {
	t.Helper()

	nodeManager := server.NewNodeManager(
		server.WithStoreFactory(storeFactory),
	)
	router := mux.NewRouter()
	mux.UseAuthentication(router, mux.WithAuthValidator(validator.Validate))
	mux.UseAuthorization(router)
	router.Healthz().AllowAnonymous()
	ConfigureWebSocketServer(router, nodeManager)

	s := &liveWebSocketServer{
		server:  httptest.NewServer(router),
		manager: nodeManager,
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

func newLiveConsumeStore(entries []*api.Entry, gateAfterIndex int) *liveConsumeStore {
	cloned := make([]*api.Entry, 0, len(entries))
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		copyEntry := *entry
		cloned = append(cloned, &copyEntry)
	}
	return &liveConsumeStore{
		entries:        cloned,
		gateAfterIndex: gateAfterIndex,
		gateAvailable:  true,
		gateStarted:    make(chan struct{}),
		releaseGate:    make(chan struct{}),
	}
}

func (s *liveConsumeStore) GetSpaces(context.Context) enumerators.Enumerator[string] {
	spaces := make([]string, 0)
	seen := make(map[string]struct{})
	for _, entry := range s.entries {
		if entry == nil {
			continue
		}
		if _, ok := seen[entry.Space]; ok {
			continue
		}
		seen[entry.Space] = struct{}{}
		spaces = append(spaces, entry.Space)
	}
	sort.Strings(spaces)
	return enumerators.Slice(spaces)
}

func (s *liveConsumeStore) GetSegments(ctx context.Context, space string) enumerators.Enumerator[string] {
	segments := make([]string, 0)
	seen := make(map[string]struct{})
	for _, entry := range s.entries {
		if entry == nil || entry.Space != space {
			continue
		}
		if _, ok := seen[entry.Segment]; ok {
			continue
		}
		seen[entry.Segment] = struct{}{}
		segments = append(segments, entry.Segment)
	}
	sort.Strings(segments)
	return enumerators.Slice(segments)
}

func (s *liveConsumeStore) ConsumeSpace(ctx context.Context, args *api.ConsumeSpace) enumerators.Enumerator[*api.Entry] {
	items := make([]*api.Entry, 0)
	offsetHex := ""
	if args != nil {
		offsetHex = args.Offset.ToHexString()
	}
	for _, entry := range s.entries {
		if entry == nil || args == nil || entry.Space != args.Space {
			continue
		}
		if args.MinTimestamp > 0 && entry.Timestamp < args.MinTimestamp {
			continue
		}
		if args.MaxTimestamp > 0 && entry.Timestamp > args.MaxTimestamp {
			continue
		}
		if offsetHex != "" && entry.GetSpaceOffset().ToHexString() <= offsetHex {
			continue
		}
		items = append(items, cloneAPIEntry(entry))
	}
	sortLiveEntries(items)
	return &liveConsumeEnumerator{ctx: ctx, items: items, gate: s.acquireGate()}
}

func (s *liveConsumeStore) ConsumeSegment(ctx context.Context, args *api.ConsumeSegment) enumerators.Enumerator[*api.Entry] {
	items := make([]*api.Entry, 0)
	for _, entry := range s.entries {
		if entry == nil || args == nil {
			continue
		}
		if entry.Space != args.Space || entry.Segment != args.Segment {
			continue
		}
		if args.MinSequence > 0 && entry.Sequence < args.MinSequence {
			continue
		}
		if args.MaxSequence > 0 && entry.Sequence > args.MaxSequence {
			continue
		}
		if args.MinTimestamp > 0 && entry.Timestamp < args.MinTimestamp {
			continue
		}
		if args.MaxTimestamp > 0 && entry.Timestamp > args.MaxTimestamp {
			continue
		}
		items = append(items, cloneAPIEntry(entry))
	}
	sortLiveEntries(items)
	return &liveConsumeEnumerator{ctx: ctx, items: items, gate: s.acquireGate()}
}

func (s *liveConsumeStore) Consume(ctx context.Context, args *api.Consume) enumerators.Enumerator[*api.Entry] {
	items := make([]*api.Entry, 0)
	for _, entry := range s.entries {
		if entry == nil || args == nil {
			continue
		}
		offset, ok := args.Offsets[entry.Space]
		if !ok {
			continue
		}
		if args.MinTimestamp > 0 && entry.Timestamp < args.MinTimestamp {
			continue
		}
		if args.MaxTimestamp > 0 && entry.Timestamp > args.MaxTimestamp {
			continue
		}
		offsetHex := offset.ToHexString()
		if offsetHex != "" && entry.GetSpaceOffset().ToHexString() <= offsetHex {
			continue
		}
		items = append(items, cloneAPIEntry(entry))
	}
	sortLiveEntries(items)
	return &liveConsumeEnumerator{ctx: ctx, items: items, gate: s.acquireGate()}
}

func (s *liveConsumeStore) Peek(ctx context.Context, space, segment string) (*api.Entry, error) {
	var latest *api.Entry
	for _, entry := range s.entries {
		if entry == nil || entry.Space != space || entry.Segment != segment {
			continue
		}
		if latest == nil ||
			entry.Timestamp > latest.Timestamp ||
			(entry.Timestamp == latest.Timestamp && entry.Sequence > latest.Sequence) {
			latest = cloneAPIEntry(entry)
		}
	}
	return latest, nil
}

func (s *liveConsumeStore) Produce(context.Context, *api.Produce, enumerators.Enumerator[*api.Record]) enumerators.Enumerator[*api.SegmentStatus] {
	return enumerators.Slice([]*api.SegmentStatus{})
}

func (s *liveConsumeStore) Close() {}

func (s *liveConsumeStore) acquireGate() *liveConsumeGate {
	s.gateMu.Lock()
	defer s.gateMu.Unlock()
	if !s.gateAvailable {
		return nil
	}
	s.gateAvailable = false
	return &liveConsumeGate{
		started:         s.gateStarted,
		release:         s.releaseGate,
		blockAfterIndex: s.gateAfterIndex,
	}
}

func (e *liveConsumeEnumerator) MoveNext() bool {
	if e.gate != nil && e.idx == e.gate.blockAfterIndex {
		e.gate.once.Do(func() {
			close(e.gate.started)
		})
		<-e.gate.release
		e.gate = nil
	}
	if e.idx >= len(e.items) {
		return false
	}
	e.idx++
	return true
}

func (e *liveConsumeEnumerator) Current() (*api.Entry, error) {
	if e.idx == 0 || e.idx > len(e.items) {
		return nil, context.Canceled
	}
	return cloneAPIEntry(e.items[e.idx-1]), nil
}

func (e *liveConsumeEnumerator) Dispose() {}

func (e *liveConsumeEnumerator) Err() error { return nil }

func cloneAPIEntry(entry *api.Entry) *api.Entry {
	if entry == nil {
		return nil
	}
	cloned := *entry
	return &cloned
}

func sortLiveEntries(entries []*api.Entry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Timestamp != entries[j].Timestamp {
			return entries[i].Timestamp < entries[j].Timestamp
		}
		if entries[i].Space != entries[j].Space {
			return entries[i].Space < entries[j].Space
		}
		if entries[i].Segment != entries[j].Segment {
			return entries[i].Segment < entries[j].Segment
		}
		return entries[i].Sequence < entries[j].Sequence
	})
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

func waitForSubscriptionSegmentsAtLeast(t *testing.T, updates <-chan client.SegmentStatus, expected map[string]uint64, timeout time.Duration) map[string]client.SegmentStatus {
	t.Helper()

	seen := make(map[string]client.SegmentStatus, len(expected))
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		complete := true
		for segment, minSequence := range expected {
			status, ok := seen[segment]
			if !ok || status.LastSequence < minSequence {
				complete = false
				break
			}
		}
		if complete {
			return seen
		}

		select {
		case status := <-updates:
			if status.Heartbeat {
				continue
			}
			current, ok := seen[status.Segment]
			if !ok || status.LastSequence > current.LastSequence {
				seen[status.Segment] = status
			}
		case <-deadline.C:
			t.Fatalf("timeout waiting for subscription segments %+v; last seen=%v", expected, seen)
		}
	}
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

type liveConsumeCase struct {
	name           string
	entries        []*api.Entry
	expected       []string
	gateAfterIndex int
	openEnum       func(client.Client, context.Context, uuid.UUID) enumerators.Enumerator[*client.Entry]
	retryEnum      func(client.Client, context.Context, uuid.UUID, *client.Entry) enumerators.Enumerator[*client.Entry]
}

func liveConsumeCases() []liveConsumeCase {
	return []liveConsumeCase{
		{
			name: "consume-segment",
			entries: []*api.Entry{
				{Space: "space", Segment: "segment", Sequence: 1, Timestamp: 100, Payload: []byte("one")},
				{Space: "space", Segment: "segment", Sequence: 2, Timestamp: 200, Payload: []byte("two")},
				{Space: "space", Segment: "segment", Sequence: 3, Timestamp: 300, Payload: []byte("three")},
			},
			expected: []string{
				"space/segment:1",
				"space/segment:2",
				"space/segment:3",
			},
			gateAfterIndex: 1,
			openEnum: func(c client.Client, ctx context.Context, storeID uuid.UUID) enumerators.Enumerator[*client.Entry] {
				return c.ConsumeSegment(ctx, storeID, &client.ConsumeSegment{
					Space:   "space",
					Segment: "segment",
				})
			},
			retryEnum: func(c client.Client, ctx context.Context, storeID uuid.UUID, last *client.Entry) enumerators.Enumerator[*client.Entry] {
				return c.ConsumeSegment(ctx, storeID, &client.ConsumeSegment{
					Space:       "space",
					Segment:     "segment",
					MinSequence: last.Sequence + 1,
				})
			},
		},
		{
			name: "consume-space",
			entries: []*api.Entry{
				{Space: "space", Segment: "seg-a", Sequence: 1, Timestamp: 100, Payload: []byte("one")},
				{Space: "space", Segment: "seg-b", Sequence: 1, Timestamp: 200, Payload: []byte("two")},
				{Space: "space", Segment: "seg-a", Sequence: 2, Timestamp: 300, Payload: []byte("three")},
			},
			expected: []string{
				"space/seg-a:1",
				"space/seg-b:1",
				"space/seg-a:2",
			},
			gateAfterIndex: 1,
			openEnum: func(c client.Client, ctx context.Context, storeID uuid.UUID) enumerators.Enumerator[*client.Entry] {
				return c.ConsumeSpace(ctx, storeID, &client.ConsumeSpace{
					Space: "space",
				})
			},
			retryEnum: func(c client.Client, ctx context.Context, storeID uuid.UUID, last *client.Entry) enumerators.Enumerator[*client.Entry] {
				return c.ConsumeSpace(ctx, storeID, &client.ConsumeSpace{
					Space:  "space",
					Offset: last.GetSpaceOffset(),
				})
			},
		},
		{
			name: "consume",
			entries: []*api.Entry{
				{Space: "space", Segment: "segment", Sequence: 1, Timestamp: 100, Payload: []byte("one")},
				{Space: "space", Segment: "segment", Sequence: 2, Timestamp: 200, Payload: []byte("two")},
				{Space: "space", Segment: "segment", Sequence: 3, Timestamp: 300, Payload: []byte("three")},
			},
			expected: []string{
				"space/segment:1",
				"space/segment:2",
				"space/segment:3",
			},
			gateAfterIndex: 2,
			openEnum: func(c client.Client, ctx context.Context, storeID uuid.UUID) enumerators.Enumerator[*client.Entry] {
				return c.Consume(ctx, storeID, &client.Consume{
					Offsets: map[string]lexkey.LexKey{
						"space": {},
					},
				})
			},
			retryEnum: func(c client.Client, ctx context.Context, storeID uuid.UUID, last *client.Entry) enumerators.Enumerator[*client.Entry] {
				return c.Consume(ctx, storeID, &client.Consume{
					Offsets: map[string]lexkey.LexKey{
						last.Space: last.GetSpaceOffset(),
					},
				})
			},
		},
	}
}

func readNextLiveEntry(t *testing.T, enum enumerators.Enumerator[*client.Entry]) *client.Entry {
	t.Helper()
	require.True(t, enum.MoveNext())
	entry, err := enum.Current()
	require.NoError(t, err)
	require.NotNil(t, entry)
	return entry
}

func liveEntryLabel(entry *client.Entry) string {
	if entry == nil {
		return ""
	}
	return fmt.Sprintf("%s/%s:%d", entry.Space, entry.Segment, entry.Sequence)
}

func runLiveConsumeRetryScenario(t *testing.T, tc liveConsumeCase, disruption string) {
	t.Helper()

	store := newLiveConsumeStore(tc.entries, tc.gateAfterIndex)
	factory := &liveConsumeStoreFactory{store: store}

	secretA := []byte("consume-secret-a-" + tc.name + "-" + disruption)
	secretB := []byte("consume-secret-b-" + tc.name + "-" + disruption)
	validator := &jwtkit.HMAC256Validator{Secret: secretA}
	server := newLiveWebSocketServerWithStoreFactory(t, factory, validator)

	tokenA := signWebSocketToken(t, secretA, ScopeAllStores)
	tokenB := signWebSocketToken(t, secretB, ScopeAllStores)

	var currentBaseURL atomic.Value
	currentBaseURL.Store(server.URL())

	var currentToken atomic.Value
	currentToken.Store(tokenA)

	provider := NewBidiStreamProvider(webSocketBaseURL(server.URL()), func() (string, error) {
		return currentToken.Load().(string), nil
	}).(*WebSocketBidiStreamProvider)
	provider.dialFn = func() (*websocket.Conn, error) {
		return dialAuthorizedWebSocket(currentBaseURL.Load().(string), currentToken.Load().(string))
	}

	var dialFailures atomic.Int32
	if disruption == "auth-refresh" {
		provider.RetryAuthFailures = true
		provider.OnDialFailure = func(err error) {
			if err != nil {
				dialFailures.Add(1)
				currentToken.Store(tokenB)
			}
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

	enum := tc.openEnum(clientInstance, ctx, storeID)
	defer enum.Dispose()

	first := readNextLiveEntry(t, enum)
	got := []string{liveEntryLabel(first)}

	select {
	case <-store.gateStarted:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for consume stream to block before the second entry")
	}

	switch disruption {
	case "websocket-drop":
		closeCurrentConn(t, provider)
	case "server-restart":
		closeCurrentConn(t, provider)
		server.Close()
		restarted := newLiveWebSocketServerWithStoreFactory(t, factory, &jwtkit.HMAC256Validator{Secret: secretA})
		currentBaseURL.Store(restarted.URL())
	case "auth-refresh":
		validator.Secret = secretB
		closeCurrentConn(t, provider)
	default:
		t.Fatalf("unknown disruption %q", disruption)
	}

	close(store.releaseGate)

	assert.False(t, enum.MoveNext())
	require.Error(t, enum.Err())
	require.Eventually(t, func() bool {
		return listener.count() >= 1
	}, 10*time.Second, 50*time.Millisecond)
	if disruption == "auth-refresh" {
		require.Eventually(t, func() bool {
			return dialFailures.Load() >= 1
		}, 10*time.Second, 50*time.Millisecond)
	}

	retryCtx, retryCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer retryCancel()
	retryEnum := tc.retryEnum(clientInstance, retryCtx, storeID, first)
	defer retryEnum.Dispose()

	for retryEnum.MoveNext() {
		entry, err := retryEnum.Current()
		require.NoError(t, err)
		got = append(got, liveEntryLabel(entry))
	}
	require.NoError(t, retryEnum.Err())
	assert.Equal(t, tc.expected, got)
}

func TestShouldLiveConsumeEnumeratorsRequireRetryAfterWebSocketDrop(t *testing.T) {
	for _, tc := range liveConsumeCases() {
		t.Run(tc.name, func(t *testing.T) {
			runLiveConsumeRetryScenario(t, tc, "websocket-drop")
		})
	}
}

func TestShouldLiveConsumeEnumeratorsRequireRetryAfterServerRestart(t *testing.T) {
	for _, tc := range liveConsumeCases() {
		t.Run(tc.name, func(t *testing.T) {
			runLiveConsumeRetryScenario(t, tc, "server-restart")
		})
	}
}

func TestShouldLiveConsumeEnumeratorsRequireRetryAfterAuthFailureOnReconnect(t *testing.T) {
	for _, tc := range liveConsumeCases() {
		t.Run(tc.name, func(t *testing.T) {
			runLiveConsumeRetryScenario(t, tc, "auth-refresh")
		})
	}
}

func TestShouldLiveClientRecoversAfterWebSocketDrop(t *testing.T) {
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

func TestShouldLiveClientRecoversAfterServerRestart(t *testing.T) {
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

func TestShouldLiveClientRefreshesTokenAfterAuthFailureOnReconnect(t *testing.T) {
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

func TestShouldLiveSubscriptionRequiresRecreateAfterAuthFailureOnReconnect(t *testing.T) {
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
	route := api.GetSegmentNotificationRoute(storeID, "space")
	require.Eventually(t, func() bool {
		return server.bus.subscriberCount(route) == 0
	}, 10*time.Second, 25*time.Millisecond)

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

	recreatedUpdates := make(chan client.SegmentStatus, 32)
	recreatedSub, err := clientInstance.SubscribeToSegment(context.Background(), storeID, "space", "segment", func(status *client.SegmentStatus) {
		select {
		case recreatedUpdates <- *status:
		default:
		}
	})
	require.NoError(t, err)
	t.Cleanup(recreatedSub.Unsubscribe)
	waitForLiveSubscriptionRegistration(t, server.bus, storeID, "space", 2)

	second := waitForSubscriptionSequenceAtLeast(t, recreatedUpdates, 2, 10*time.Second)
	assert.Equal(t, uint64(2), second.LastSequence)
}

func TestShouldLiveSpaceSubscriptionRequiresRecreateAfterAuthFailureOnReconnect(t *testing.T) {
	secretA := []byte("space-subscription-rotate-secret-a")
	secretB := []byte("space-subscription-rotate-secret-b")
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
	updates := make(chan client.SegmentStatus, 128)
	sub, err := clientInstance.SubscribeToSpace(context.Background(), storeID, "space", func(status *client.SegmentStatus) {
		if status == nil || status.Heartbeat {
			return
		}
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
	require.NoError(t, writerClient.Publish(ctx, storeID, "space", "seg-a", []byte("first-a"), nil))

	initial := waitForSubscriptionSegmentsAtLeast(t, updates, map[string]uint64{
		"seg-a": 1,
	}, 10*time.Second)
	assert.Equal(t, uint64(1), initial["seg-a"].LastSequence)

	allowReconnect := make(chan struct{})
	provider.dialFn = func() (*websocket.Conn, error) {
		<-allowReconnect
		return dialAuthorizedWebSocket(server.URL(), currentToken.Load().(string))
	}

	validator.Secret = secretB
	closeCurrentConn(t, provider)
	route := api.GetSegmentNotificationRoute(storeID, "space")
	require.Eventually(t, func() bool {
		return server.bus.subscriberCount(route) == 0
	}, 10*time.Second, 25*time.Millisecond)

	writerAfterRotate := newLiveClientWithRetry(t, server.URL(), func() (string, error) {
		return tokenB, nil
	})
	publishCtx, publishCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer publishCancel()
	require.NoError(t, writerAfterRotate.Publish(publishCtx, storeID, "space", "seg-a", []byte("second-a"), nil))
	require.NoError(t, writerAfterRotate.Publish(publishCtx, storeID, "space", "seg-b", []byte("first-b"), nil))

	close(allowReconnect)

	require.Eventually(t, func() bool {
		return dialFailures.Load() >= 1
	}, 10*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool {
		return listener.count() >= 1
	}, 10*time.Second, 50*time.Millisecond)

	recreatedUpdates := make(chan client.SegmentStatus, 128)
	recreatedSub, err := clientInstance.SubscribeToSpace(context.Background(), storeID, "space", func(status *client.SegmentStatus) {
		if status == nil || status.Heartbeat {
			return
		}
		select {
		case recreatedUpdates <- *status:
		default:
		}
	})
	require.NoError(t, err)
	t.Cleanup(recreatedSub.Unsubscribe)
	waitForLiveSubscriptionRegistration(t, server.bus, storeID, "space", 2)

	recovered := waitForSubscriptionSegmentsAtLeast(t, recreatedUpdates, map[string]uint64{
		"seg-a": 2,
		"seg-b": 1,
	}, 10*time.Second)
	assert.Equal(t, uint64(2), recovered["seg-a"].LastSequence)
	assert.Equal(t, uint64(1), recovered["seg-b"].LastSequence)
}

func TestShouldLiveSubscriptionRequiresRecreateAfterWebSocketDrop(t *testing.T) {
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
	route := api.GetSegmentNotificationRoute(storeID, "space")
	require.Eventually(t, func() bool {
		return server.bus.subscriberCount(route) == 0
	}, 10*time.Second, 25*time.Millisecond)
	publishCtx, publishCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer publishCancel()
	require.NoError(t, writerClient.Publish(publishCtx, storeID, "space", "segment", []byte("second"), nil))
	close(allowReconnect)
	require.Eventually(t, func() bool {
		return listener.count() >= 1
	}, 10*time.Second, 50*time.Millisecond)

	recreatedUpdates := make(chan client.SegmentStatus, 32)
	recreatedSub, err := clientInstance.SubscribeToSegment(context.Background(), storeID, "space", "segment", func(status *client.SegmentStatus) {
		select {
		case recreatedUpdates <- *status:
		default:
		}
	})
	require.NoError(t, err)
	t.Cleanup(recreatedSub.Unsubscribe)
	waitForLiveSubscriptionRegistration(t, server.bus, storeID, "space", 2)

	second := waitForSubscriptionSequenceAtLeast(t, recreatedUpdates, 2, 10*time.Second)
	assert.Equal(t, uint64(2), second.LastSequence)
}

func TestShouldLiveSubscriptionRequiresRecreateAfterServerRestart(t *testing.T) {
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

	recreatedUpdates := make(chan client.SegmentStatus, 32)
	recreatedSub, err := clientInstance.SubscribeToSegment(context.Background(), storeID, "space", "segment", func(status *client.SegmentStatus) {
		select {
		case recreatedUpdates <- *status:
		default:
		}
	})
	require.NoError(t, err)
	t.Cleanup(recreatedSub.Unsubscribe)
	waitForLiveSubscriptionRegistration(t, restarted.bus, storeID, "space", 1)

	second := waitForSubscriptionSequenceAtLeast(t, recreatedUpdates, 2, 10*time.Second)
	assert.Equal(t, uint64(2), second.LastSequence)
}

func TestShouldLiveSpaceSubscriptionRequiresRecreateAfterWebSocketDrop(t *testing.T) {
	secret := []byte("space-subscription-drop-secret")
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
	updates := make(chan client.SegmentStatus, 128)
	sub, err := clientInstance.SubscribeToSpace(context.Background(), storeID, "space", func(status *client.SegmentStatus) {
		if status == nil || status.Heartbeat {
			return
		}
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
	require.NoError(t, writerClient.Publish(ctx, storeID, "space", "seg-a", []byte("first-a"), nil))
	require.NoError(t, writerClient.Publish(ctx, storeID, "space", "seg-b", []byte("first-b"), nil))

	initial := waitForSubscriptionSegmentsAtLeast(t, updates, map[string]uint64{
		"seg-a": 1,
		"seg-b": 1,
	}, 10*time.Second)
	assert.Equal(t, uint64(1), initial["seg-a"].LastSequence)
	assert.Equal(t, uint64(1), initial["seg-b"].LastSequence)

	allowReconnect := make(chan struct{})
	provider.dialFn = func() (*websocket.Conn, error) {
		<-allowReconnect
		return dialAuthorizedWebSocket(server.URL(), token)
	}

	route := api.GetSegmentNotificationRoute(storeID, "space")
	closeCurrentConn(t, provider)
	require.Eventually(t, func() bool {
		return server.bus.subscriberCount(route) == 0
	}, 10*time.Second, 25*time.Millisecond)

	gapCtx, gapCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer gapCancel()
	require.NoError(t, writerClient.Publish(gapCtx, storeID, "space", "seg-a", []byte("second-a"), nil))
	require.NoError(t, writerClient.Publish(gapCtx, storeID, "space", "seg-b", []byte("second-b"), nil))
	require.NoError(t, writerClient.Publish(gapCtx, storeID, "space", "seg-c", []byte("first-c"), nil))

	close(allowReconnect)
	require.Eventually(t, func() bool {
		return listener.count() >= 1
	}, 10*time.Second, 50*time.Millisecond)

	recreatedUpdates := make(chan client.SegmentStatus, 128)
	recreatedSub, err := clientInstance.SubscribeToSpace(context.Background(), storeID, "space", func(status *client.SegmentStatus) {
		if status == nil || status.Heartbeat {
			return
		}
		select {
		case recreatedUpdates <- *status:
		default:
		}
	})
	require.NoError(t, err)
	t.Cleanup(recreatedSub.Unsubscribe)
	waitForLiveSubscriptionRegistration(t, server.bus, storeID, "space", 2)

	recovered := waitForSubscriptionSegmentsAtLeast(t, recreatedUpdates, map[string]uint64{
		"seg-a": 2,
		"seg-b": 2,
		"seg-c": 1,
	}, 10*time.Second)
	assert.Equal(t, uint64(2), recovered["seg-a"].LastSequence)
	assert.Equal(t, uint64(2), recovered["seg-b"].LastSequence)
	assert.Equal(t, uint64(1), recovered["seg-c"].LastSequence)
}
