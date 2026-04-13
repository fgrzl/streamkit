package wskit

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgrzl/claims/jwtkit"
	"github.com/fgrzl/lexkey"
	"github.com/fgrzl/mux"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/fgrzl/streamkit/pkg/server"
	"github.com/fgrzl/streamkit/pkg/storage"
	"github.com/fgrzl/streamkit/pkg/storage/azurekit"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/websocket"
)

const (
	localChaosSoakEnv                 = "STREAMKIT_SOAK"
	localChaosSoakDurationEnv         = "STREAMKIT_SOAK_DURATION"
	localChaosSoakSeedEnv             = "STREAMKIT_SOAK_SEED"
	localChaosSoakAzureEndpointEnv    = "STREAMKIT_SOAK_AZURE_ENDPOINT"
	localChaosSoakAzureAccountNameEnv = "STREAMKIT_SOAK_AZURE_ACCOUNT_NAME"
	localChaosSoakAzureAccountKeyEnv  = "STREAMKIT_SOAK_AZURE_ACCOUNT_KEY"

	defaultLocalChaosSoakDuration         = time.Minute
	defaultLocalChaosSoakAzureEndpoint    = "http://127.0.0.1:10002/devstoreaccount1"
	defaultLocalChaosSoakAzureAccountName = "devstoreaccount1"
	defaultLocalChaosSoakAzureAccountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

	localChaosSoakCatchUpTimeout = 20 * time.Second
	localChaosSoakProgressWindow = 15 * time.Second
)

type localChaosSoakConfig struct {
	duration    time.Duration
	seed        int64
	endpoint    string
	accountName string
	accountKey  string
}

type localChaosAuthState struct {
	mu           sync.Mutex
	currentToken string
	pendingToken string
}

type localChaosSoakState struct {
	mu                 sync.Mutex
	producedLatest     map[string]uint64
	consumedLatest     map[string]uint64
	subscriptionLatest map[string]uint64
	seenConsumeEntries map[string]struct{}
	disruptionCounts   map[string]int
	totalProduced      uint64
	totalConsumed      uint64
	totalSubscriptions uint64
	lastProduceUnix    atomic.Int64
	lastConsumeUnix    atomic.Int64
	lastSubUnix        atomic.Int64
}

func TestShouldLocalWebSocketChaosSoak(t *testing.T) {
	cfg := loadLocalChaosSoakConfig(t)
	rng := rand.New(rand.NewSource(cfg.seed))
	t.Logf("local websocket chaos soak duration=%s seed=%d endpoint=%s", cfg.duration, cfg.seed, cfg.endpoint)

	const subscribedSpace = "space-a"

	targets := []struct {
		space   string
		segment string
	}{
		{space: "space-a", segment: "seg-a"},
		{space: "space-a", segment: "seg-b"},
		{space: "space-b", segment: "seg-a"},
		{space: "space-b", segment: "seg-b"},
	}

	storeFactory := newLocalChaosAzureStoreFactory(t, cfg, "soak"+uuid.NewString())
	validator := &jwtkit.HMAC256Validator{Secret: []byte(fmt.Sprintf("local-chaos-secret-%d", cfg.seed))}
	currentServer := newLocalChaosAzureWebSocketServer(t, storeFactory, validator)

	var currentBaseURL atomic.Value
	currentBaseURL.Store(currentServer.URL())

	authState := newLocalChaosAuthState(signWebSocketToken(t, validator.Secret, ScopeAllStores))
	var dialFailures atomic.Int32
	var slowSubscriptionUntil atomic.Int64

	observerProvider := newLocalChaosSoakProvider(&currentBaseURL, authState, &dialFailures)
	listener := &testReconnectListener{}
	observerProvider.RegisterReconnectListener(listener)
	observerClient := client.NewClientWithRetryPolicy(observerProvider, liveRetryPolicy())
	t.Cleanup(func() {
		assert.NoError(t, observerClient.Close())
		assert.NoError(t, observerProvider.Close())
	})

	writerProvider := newLocalChaosSoakProvider(&currentBaseURL, authState, &dialFailures)
	writerClient := client.NewClientWithRetryPolicy(writerProvider, liveRetryPolicy())
	t.Cleanup(func() {
		assert.NoError(t, writerClient.Close())
		assert.NoError(t, writerProvider.Close())
	})

	state := newLocalChaosSoakState()
	storeID := uuid.New()
	sub, err := observerClient.SubscribeToSpace(context.Background(), storeID, subscribedSpace, func(status *client.SegmentStatus) {
		if status == nil || status.Heartbeat {
			return
		}
		if until := slowSubscriptionUntil.Load(); until > 0 && time.Now().Before(time.Unix(0, until)) {
			time.Sleep(75 * time.Millisecond)
		}
		state.recordSubscription(status)
	})
	require.NoError(t, err)
	t.Cleanup(sub.Unsubscribe)
	waitForLiveSubscriptionRegistration(t, currentServer.bus, storeID, subscribedSpace, 1)

	for _, target := range targets {
		require.NoError(t, publishLocalChaosRecord(context.Background(), writerClient, storeID, target.space, target.segment, state))
	}
	require.Eventually(t, func() bool {
		return state.subscriptionCaughtUp(state.snapshotProducedForSpace(subscribedSpace))
	}, 10*time.Second, 50*time.Millisecond)

	testCtx, cancelTest := context.WithTimeout(context.Background(), cfg.duration+localChaosSoakCatchUpTimeout+10*time.Second)
	defer cancelTest()

	producerCtx, stopProducer := context.WithCancel(testCtx)
	consumeCtx, stopConsume := context.WithCancel(testCtx)
	defer stopProducer()
	defer stopConsume()

	errCh := make(chan error, 2)
	var producerWG sync.WaitGroup
	var consumerWG sync.WaitGroup

	producerWG.Add(1)
	go func() {
		defer producerWG.Done()
		runLocalChaosProducerLoop(producerCtx, writerClient, storeID, targets, state)
	}()

	consumerWG.Add(1)
	go func() {
		defer consumerWG.Done()
		runLocalChaosConsumeLoop(consumeCtx, observerClient, storeID, state, errCh)
	}()

	require.Eventually(t, func() bool {
		return state.totalConsumedCount() >= uint64(len(targets))
	}, 10*time.Second, 50*time.Millisecond)

	soakDeadline := time.Now().Add(cfg.duration)
	for time.Now().Before(soakDeadline) {
		failOnLocalChaosError(t, errCh)
		assertLocalChaosProgress(t, state)

		action := rng.Intn(4)
		switch action {
		case 0:
			state.incrementDisruption("websocket-drop")
			reconnects := listener.count()
			subscribeCalls := currentServer.bus.totalSubscribeCalls()
			closeCurrentConn(t, observerProvider)
			waitForLocalChaosRecovery(t, currentServer, storeID, subscribedSpace, reconnects, subscribeCalls, listener)

		case 1:
			state.incrementDisruption("auth-refresh")
			reconnects := listener.count()
			subscribeCalls := currentServer.bus.totalSubscribeCalls()
			nextSecret := []byte(fmt.Sprintf("local-chaos-secret-%d", time.Now().UnixNano()))
			authState.schedule(signWebSocketToken(t, nextSecret, ScopeAllStores))
			validator.Secret = nextSecret
			closeCurrentConn(t, observerProvider)
			waitForLocalChaosRecovery(t, currentServer, storeID, subscribedSpace, reconnects, subscribeCalls, listener)
			require.Eventually(t, func() bool {
				return !authState.hasPending()
			}, 10*time.Second, 25*time.Millisecond)

		case 2:
			state.incrementDisruption("server-restart")
			reconnects := listener.count()
			closeCurrentConn(t, observerProvider)
			currentServer.Close()
			restarted := newLocalChaosAzureWebSocketServer(t, storeFactory, validator)
			currentServer = restarted
			currentBaseURL.Store(restarted.URL())
			require.Eventually(t, func() bool {
				return listener.count() > reconnects
			}, 10*time.Second, 50*time.Millisecond)
			waitForLiveSubscriptionRegistration(t, restarted.bus, storeID, subscribedSpace, 1)

		case 3:
			state.incrementDisruption("slow-handler")
			slowSubscriptionUntil.Store(time.Now().Add(300 * time.Millisecond).UnixNano())
		}

		failOnLocalChaosError(t, errCh)
		assertLocalChaosProgress(t, state)

		sleepFor := time.Duration(250+rng.Intn(300)) * time.Millisecond
		timer := time.NewTimer(sleepFor)
		select {
		case <-timer.C:
		case <-testCtx.Done():
			timer.Stop()
			t.Fatalf("local chaos soak timed out before completion: %v", testCtx.Err())
		}
	}

	stopProducer()
	slowSubscriptionUntil.Store(0)
	waitForLocalChaosLoopExit(t, &producerWG, errCh, false)

	waitForLiveSubscriptionRegistration(t, currentServer.bus, storeID, subscribedSpace, 1)

	producedAll := state.snapshotProducedAll()
	producedSubscribed := state.snapshotProducedForSpace(subscribedSpace)
	require.Eventually(t, func() bool {
		failOnLocalChaosError(t, errCh)
		return state.consumeCaughtUp(producedAll)
	}, localChaosSoakCatchUpTimeout, 100*time.Millisecond)
	require.Eventually(t, func() bool {
		failOnLocalChaosError(t, errCh)
		return state.subscriptionCaughtUp(producedSubscribed)
	}, localChaosSoakCatchUpTimeout, 100*time.Millisecond)

	stopConsume()
	waitForLocalChaosLoopExit(t, &consumerWG, errCh, true)

	t.Logf(
		"local websocket chaos soak complete seed=%d duration=%s produced=%d consumed=%d subscription_updates=%d reconnects=%d dial_failures=%d disruptions=%v",
		cfg.seed,
		cfg.duration,
		state.totalProducedCount(),
		state.totalConsumedCount(),
		state.totalSubscriptionCount(),
		listener.count(),
		dialFailures.Load(),
		state.snapshotDisruptions(),
	)
}

func loadLocalChaosSoakConfig(t *testing.T) localChaosSoakConfig {
	t.Helper()
	if testing.Short() {
		t.Skip("local chaos soak skipped in short mode")
	}
	if os.Getenv(localChaosSoakEnv) == "" {
		t.Skipf("set %s=1 to run the local Azure-backed websocket chaos soak", localChaosSoakEnv)
	}

	cfg := localChaosSoakConfig{
		duration:    defaultLocalChaosSoakDuration,
		seed:        time.Now().UnixNano(),
		endpoint:    getenvOrDefault(localChaosSoakAzureEndpointEnv, defaultLocalChaosSoakAzureEndpoint),
		accountName: getenvOrDefault(localChaosSoakAzureAccountNameEnv, defaultLocalChaosSoakAzureAccountName),
		accountKey:  getenvOrDefault(localChaosSoakAzureAccountKeyEnv, defaultLocalChaosSoakAzureAccountKey),
	}

	if raw := os.Getenv(localChaosSoakDurationEnv); raw != "" {
		duration, err := time.ParseDuration(raw)
		require.NoError(t, err, "invalid %s", localChaosSoakDurationEnv)
		require.Greater(t, duration, 5*time.Second, "%s must be greater than 5s", localChaosSoakDurationEnv)
		cfg.duration = duration
	}
	if raw := os.Getenv(localChaosSoakSeedEnv); raw != "" {
		seed, err := strconv.ParseInt(raw, 10, 64)
		require.NoError(t, err, "invalid %s", localChaosSoakSeedEnv)
		cfg.seed = seed
	}

	return cfg
}

func getenvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func newLocalChaosAzureStoreFactory(t *testing.T, cfg localChaosSoakConfig, prefix string) *azurekit.StoreFactory {
	t.Helper()

	factory, err := azurekit.NewStoreFactory(context.Background(), &azurekit.AzureStoreOptions{
		Prefix:            prefix,
		AccountName:       cfg.accountName,
		AccountKey:        cfg.accountKey,
		Endpoint:          cfg.endpoint,
		AllowInsecureHTTP: strings.HasPrefix(strings.ToLower(cfg.endpoint), "http://"),
	})
	require.NoError(t, err, "failed to create Azure store factory; ensure the local Azure emulator is running")
	return factory
}

func newLocalChaosAzureWebSocketServer(t *testing.T, storeFactory storage.StoreFactory, validator *jwtkit.HMAC256Validator) *liveWebSocketServer {
	t.Helper()

	messageBus := newLiveMessageBus()
	nodeManager := server.NewNodeManager(
		server.WithStoreFactory(storeFactory),
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

func newLocalChaosSoakProvider(baseURL *atomic.Value, auth *localChaosAuthState, dialFailures *atomic.Int32) *WebSocketBidiStreamProvider {
	provider := NewBidiStreamProvider(webSocketBaseURL(baseURL.Load().(string)), auth.fetchJWT).(*WebSocketBidiStreamProvider)
	provider.RetryAuthFailures = true
	provider.OnDialFailure = func(err error) {
		if err == nil {
			return
		}
		dialFailures.Add(1)
		auth.promotePending()
	}
	provider.dialFn = func() (*websocket.Conn, error) {
		return dialAuthorizedWebSocket(baseURL.Load().(string), auth.current())
	}
	return provider
}

func newLocalChaosAuthState(token string) *localChaosAuthState {
	return &localChaosAuthState{currentToken: token}
}

func (s *localChaosAuthState) fetchJWT() (string, error) {
	return s.current(), nil
}

func (s *localChaosAuthState) current() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentToken
}

func (s *localChaosAuthState) schedule(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingToken = token
}

func (s *localChaosAuthState) promotePending() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingToken == "" {
		return false
	}
	s.currentToken = s.pendingToken
	s.pendingToken = ""
	return true
}

func (s *localChaosAuthState) hasPending() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingToken != ""
}

func newLocalChaosSoakState() *localChaosSoakState {
	now := time.Now().UnixNano()
	state := &localChaosSoakState{
		producedLatest:     make(map[string]uint64),
		consumedLatest:     make(map[string]uint64),
		subscriptionLatest: make(map[string]uint64),
		seenConsumeEntries: make(map[string]struct{}),
		disruptionCounts:   make(map[string]int),
	}
	state.lastProduceUnix.Store(now)
	state.lastConsumeUnix.Store(now)
	state.lastSubUnix.Store(now)
	return state
}

func (s *localChaosSoakState) recordProduced(space, segment string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := localChaosSegmentKey(space, segment)
	s.producedLatest[key]++
	s.totalProduced++
	s.lastProduceUnix.Store(time.Now().UnixNano())
	return s.producedLatest[key]
}

func (s *localChaosSoakState) recordConsumed(entry *client.Entry) error {
	if entry == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entryKey := localChaosEntryKey(entry)
	if _, exists := s.seenConsumeEntries[entryKey]; exists {
		return fmt.Errorf("duplicate consume entry observed: %s", entryKey)
	}
	s.seenConsumeEntries[entryKey] = struct{}{}

	segmentKey := localChaosSegmentKey(entry.Space, entry.Segment)
	if entry.Sequence > s.consumedLatest[segmentKey] {
		s.consumedLatest[segmentKey] = entry.Sequence
	}
	s.totalConsumed++
	s.lastConsumeUnix.Store(time.Now().UnixNano())
	return nil
}

func (s *localChaosSoakState) recordSubscription(status *client.SegmentStatus) {
	if status == nil || status.Heartbeat {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := localChaosSegmentKey(status.Space, status.Segment)
	if status.LastSequence > s.subscriptionLatest[key] {
		s.subscriptionLatest[key] = status.LastSequence
	}
	s.totalSubscriptions++
	s.lastSubUnix.Store(time.Now().UnixNano())
}

func (s *localChaosSoakState) incrementDisruption(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.disruptionCounts[name]++
}

func (s *localChaosSoakState) snapshotProducedAll() map[string]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneSequenceMap(s.producedLatest)
}

func (s *localChaosSoakState) snapshotProducedForSpace(space string) map[string]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make(map[string]uint64)
	prefix := space + "/"
	for key, value := range s.producedLatest {
		if strings.HasPrefix(key, prefix) {
			result[key] = value
		}
	}
	return result
}

func (s *localChaosSoakState) consumeCaughtUp(expected map[string]uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, value := range expected {
		if s.consumedLatest[key] < value {
			return false
		}
	}
	return true
}

func (s *localChaosSoakState) subscriptionCaughtUp(expected map[string]uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, value := range expected {
		if s.subscriptionLatest[key] < value {
			return false
		}
	}
	return true
}

func (s *localChaosSoakState) totalProducedCount() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalProduced
}

func (s *localChaosSoakState) totalConsumedCount() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalConsumed
}

func (s *localChaosSoakState) totalSubscriptionCount() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalSubscriptions
}

func (s *localChaosSoakState) snapshotDisruptions() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]int, len(s.disruptionCounts))
	for key, value := range s.disruptionCounts {
		result[key] = value
	}
	return result
}

func runLocalChaosProducerLoop(
	ctx context.Context,
	c client.Client,
	storeID uuid.UUID,
	targets []struct {
		space   string
		segment string
	},
	state *localChaosSoakState,
) {
	idx := 0
	for ctx.Err() == nil {
		target := targets[idx%len(targets)]
		idx++

		if err := publishLocalChaosRecord(ctx, c, storeID, target.space, target.segment, state); err != nil {
			if ctx.Err() == nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				timer := time.NewTimer(100 * time.Millisecond)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return
				}
			}
			continue
		}

		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}

func publishLocalChaosRecord(ctx context.Context, c client.Client, storeID uuid.UUID, space, segment string, state *localChaosSoakState) error {
	sequence := state.recordProduced(space, segment)
	payload := []byte(fmt.Sprintf("chaos-soak-%s-%s-%d", space, segment, sequence))

	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.Publish(callCtx, storeID, space, segment, payload, nil); err != nil {
		state.mu.Lock()
		key := localChaosSegmentKey(space, segment)
		if state.producedLatest[key] == sequence {
			state.producedLatest[key]--
			state.totalProduced--
		}
		state.mu.Unlock()
		return err
	}
	return nil
}

func runLocalChaosConsumeLoop(ctx context.Context, c client.Client, storeID uuid.UUID, state *localChaosSoakState, errCh chan<- error) {
	offsets := map[string]lexkey.LexKey{
		"space-a": {},
		"space-b": {},
	}

	for ctx.Err() == nil {
		callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		enum := c.Consume(callCtx, storeID, &client.Consume{Offsets: cloneOffsets(offsets)})
		drained := true
		for enum.MoveNext() {
			drained = false
			entry, err := enum.Current()
			if err != nil {
				cancel()
				select {
				case errCh <- fmt.Errorf("consume current failed: %w", err):
				default:
				}
				return
			}
			if err := state.recordConsumed(entry); err != nil {
				cancel()
				select {
				case errCh <- err:
				default:
				}
				return
			}
			offsets[entry.Space] = entry.GetSpaceOffset()
		}
		err := enum.Err()
		cancel()
		if err != nil && ctx.Err() == nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			if !client.IsRetryable(err) {
				select {
				case errCh <- fmt.Errorf("consume stream failed: %w", err):
				default:
				}
				return
			}
			timer := time.NewTimer(50 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return
			}
		}
		if drained {
			timer := time.NewTimer(25 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return
			}
		}
	}
}

func waitForLocalChaosRecovery(
	t *testing.T,
	currentServer *liveWebSocketServer,
	storeID uuid.UUID,
	space string,
	beforeReconnects int,
	beforeSubscribeCalls int32,
	listener *testReconnectListener,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		return listener.count() > beforeReconnects
	}, 10*time.Second, 50*time.Millisecond)

	route := api.GetSegmentNotificationRoute(storeID, space)
	require.Eventually(t, func() bool {
		if currentServer == nil || currentServer.bus == nil {
			return false
		}
		return currentServer.bus.totalSubscribeCalls() > beforeSubscribeCalls &&
			currentServer.bus.subscriberCount(route) > 0
	}, 10*time.Second, 25*time.Millisecond)
}

func failOnLocalChaosError(t *testing.T, errCh <-chan error) {
	t.Helper()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	default:
	}
}

func assertLocalChaosProgress(t *testing.T, state *localChaosSoakState) {
	t.Helper()
	now := time.Now()
	lastProduce := time.Unix(0, state.lastProduceUnix.Load())
	lastConsume := time.Unix(0, state.lastConsumeUnix.Load())
	require.LessOrEqual(t, now.Sub(lastProduce), localChaosSoakProgressWindow, "producer stalled during chaos soak")
	require.LessOrEqual(t, now.Sub(lastConsume), localChaosSoakProgressWindow, "consume reader stalled during chaos soak")
}

func waitForLocalChaosLoopExit(t *testing.T, wg *sync.WaitGroup, errCh <-chan error, drainErrors bool) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for chaos soak goroutines to stop")
	}
	if drainErrors {
		failOnLocalChaosError(t, errCh)
	}
}

func cloneOffsets(src map[string]lexkey.LexKey) map[string]lexkey.LexKey {
	dst := make(map[string]lexkey.LexKey, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func cloneSequenceMap(src map[string]uint64) map[string]uint64 {
	dst := make(map[string]uint64, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func localChaosSegmentKey(space, segment string) string {
	return space + "/" + segment
}

func localChaosEntryKey(entry *client.Entry) string {
	if entry == nil {
		return ""
	}
	return fmt.Sprintf("%s/%s:%d", entry.Space, entry.Segment, entry.Sequence)
}
