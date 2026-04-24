package server

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgrzl/streamkit/pkg/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	localNodeSoakEnv                = "STREAMKIT_NODE_SOAK"
	localNodeSoakDurationEnv        = "STREAMKIT_NODE_SOAK_DURATION"
	localNodeSoakReportIntervalEnv  = "STREAMKIT_NODE_SOAK_REPORT_INTERVAL"
	localNodeSoakBatchSizeEnv       = "STREAMKIT_NODE_SOAK_BATCH"
	localNodeSoakLoopPauseEnv       = "STREAMKIT_NODE_SOAK_LOOP_PAUSE"
	defaultLocalNodeSoakDuration    = 45 * time.Second
	defaultLocalNodeSoakReportEvery = 5 * time.Second
	defaultLocalNodeSoakBatchSize   = 10
	defaultLocalNodeSoakLoopPause   = 10 * time.Millisecond
	localNodeSoakMinPhaseDuration   = 5 * time.Second
)

type localNodeSoakConfig struct {
	duration       time.Duration
	reportInterval time.Duration
	batchSize      int
	loopPause      time.Duration
}

type localNodeSoakStoreFactory struct {
	created atomic.Int64
	closed  atomic.Int64
}

type localNodeSoakStore struct {
	mockStore
	closed *atomic.Int64
}

type localNodeSoakSnapshot struct {
	heapAlloc    uint64
	heapInuse    uint64
	heapObjects  uint64
	nodeCount    int
	failureCount int
	storesMade   int64
	storesClosed int64
	ops          int
	elapsed      time.Duration
}

func (f *localNodeSoakStoreFactory) NewStore(context.Context, uuid.UUID) (storage.Store, error) {
	f.created.Add(1)
	return &localNodeSoakStore{closed: &f.closed}, nil
}

func (s *localNodeSoakStore) Close() {
	if s.closed != nil {
		s.closed.Add(1)
	}
}

func TestShouldLocalNodeManagerRetentionSoak(t *testing.T) {
	cfg := loadLocalNodeSoakConfig(t)
	phaseDuration := cfg.duration / 3
	if phaseDuration < localNodeSoakMinPhaseDuration {
		phaseDuration = localNodeSoakMinPhaseDuration
	}

	t.Logf(
		"local node soak duration=%s phase_duration=%s report_interval=%s batch=%d loop_pause=%s",
		cfg.duration,
		phaseDuration,
		cfg.reportInterval,
		cfg.batchSize,
		cfg.loopPause,
	)

	runLocalNodeSoakPhase(t, "steady_single_store", cfg, phaseDuration, func(ctx context.Context, manager *nodeManager, _ int) {
		_, err := manager.GetOrCreate(ctx, uuid.MustParse("11111111-1111-1111-1111-111111111111"))
		require.NoError(t, err)
	}, func(t *testing.T, snap localNodeSoakSnapshot) {
		assert.Equal(t, 1, snap.nodeCount)
	})

	runLocalNodeSoakPhase(t, "retain_unique_stores", cfg, phaseDuration, func(ctx context.Context, manager *nodeManager, _ int) {
		_, err := manager.GetOrCreate(ctx, uuid.New())
		require.NoError(t, err)
	}, func(t *testing.T, snap localNodeSoakSnapshot) {
		assert.Equal(t, int(snap.storesMade), snap.nodeCount)
		assert.Zero(t, snap.storesClosed)
	})

	runLocalNodeSoakPhase(t, "remove_unique_stores", cfg, phaseDuration, func(ctx context.Context, manager *nodeManager, _ int) {
		storeID := uuid.New()
		_, err := manager.GetOrCreate(ctx, storeID)
		require.NoError(t, err)
		manager.Remove(ctx, storeID)
	}, func(t *testing.T, snap localNodeSoakSnapshot) {
		assert.Zero(t, snap.nodeCount)
		assert.Equal(t, snap.storesMade, snap.storesClosed)
	})
}

func runLocalNodeSoakPhase(
	t *testing.T,
	phaseName string,
	cfg localNodeSoakConfig,
	phaseDuration time.Duration,
	step func(context.Context, *nodeManager, int),
	verify func(*testing.T, localNodeSoakSnapshot),
) {
	t.Helper()

	manager, factory := newLocalNodeSoakManager()
	ctx := context.Background()
	start := time.Now()
	baseline := snapshotLocalNodeSoak(manager, factory, 0, 0)
	nextReport := start.Add(cfg.reportInterval)
	ops := 0

	for time.Since(start) < phaseDuration {
		for range cfg.batchSize {
			step(ctx, manager, ops)
			ops++
		}

		now := time.Now()
		if !now.Before(nextReport) {
			snap := snapshotLocalNodeSoak(manager, factory, ops, now.Sub(start))
			logLocalNodeSoakSnapshot(t, phaseName, "progress", baseline, snap)
			nextReport = now.Add(cfg.reportInterval)
		}

		if cfg.loopPause > 0 {
			time.Sleep(cfg.loopPause)
		}
	}

	finalSnap := snapshotLocalNodeSoak(manager, factory, ops, time.Since(start))
	logLocalNodeSoakSnapshot(t, phaseName, "final", baseline, finalSnap)
	if verify != nil {
		verify(t, finalSnap)
	}

	manager.Close()
	postClose := snapshotLocalNodeSoak(manager, factory, ops, time.Since(start))
	logLocalNodeSoakSnapshot(t, phaseName, "post_close", baseline, postClose)
	assert.Zero(t, postClose.nodeCount)
	assert.Zero(t, postClose.failureCount)
	assert.Equal(t, postClose.storesMade, postClose.storesClosed)
}

func newLocalNodeSoakManager() (*nodeManager, *localNodeSoakStoreFactory) {
	factory := &localNodeSoakStoreFactory{}
	manager := NewNodeManager(WithStoreFactory(factory)).(*nodeManager)
	return manager, factory
}

func snapshotLocalNodeSoak(manager *nodeManager, factory *localNodeSoakStoreFactory, ops int, elapsed time.Duration) localNodeSoakSnapshot {
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	manager.mu.RLock()
	nodeCount := len(manager.nodes)
	failureCount := len(manager.failures)
	manager.mu.RUnlock()

	return localNodeSoakSnapshot{
		heapAlloc:    mem.HeapAlloc,
		heapInuse:    mem.HeapInuse,
		heapObjects:  mem.HeapObjects,
		nodeCount:    nodeCount,
		failureCount: failureCount,
		storesMade:   factory.created.Load(),
		storesClosed: factory.closed.Load(),
		ops:          ops,
		elapsed:      elapsed,
	}
}

func logLocalNodeSoakSnapshot(t *testing.T, phaseName, label string, baseline, snap localNodeSoakSnapshot) {
	t.Helper()
	t.Logf(
		"local node soak phase=%s label=%s elapsed=%s ops=%d nodes=%d failures=%d stores_made=%d stores_closed=%d heap_alloc_mib=%.2f heap_delta_mib=%.2f heap_inuse_mib=%.2f heap_objects=%d",
		phaseName,
		label,
		snap.elapsed.Round(time.Millisecond),
		snap.ops,
		snap.nodeCount,
		snap.failureCount,
		snap.storesMade,
		snap.storesClosed,
		bytesToMiB(snap.heapAlloc),
		bytesToMiBDelta(snap.heapAlloc, baseline.heapAlloc),
		bytesToMiB(snap.heapInuse),
		snap.heapObjects,
	)
}

func loadLocalNodeSoakConfig(t *testing.T) localNodeSoakConfig {
	t.Helper()
	if testing.Short() {
		t.Skip("local node soak skipped in short mode")
	}
	if os.Getenv(localNodeSoakEnv) == "" {
		t.Skipf("set %s=1 to run the local node-manager retention soak", localNodeSoakEnv)
	}

	cfg := localNodeSoakConfig{
		duration:       defaultLocalNodeSoakDuration,
		reportInterval: defaultLocalNodeSoakReportEvery,
		batchSize:      defaultLocalNodeSoakBatchSize,
		loopPause:      defaultLocalNodeSoakLoopPause,
	}

	if raw := os.Getenv(localNodeSoakDurationEnv); raw != "" {
		duration, err := time.ParseDuration(raw)
		require.NoError(t, err, "invalid %s", localNodeSoakDurationEnv)
		require.GreaterOrEqual(t, duration, 3*localNodeSoakMinPhaseDuration, "%s must be at least %s", localNodeSoakDurationEnv, 3*localNodeSoakMinPhaseDuration)
		cfg.duration = duration
	}
	if raw := os.Getenv(localNodeSoakReportIntervalEnv); raw != "" {
		reportInterval, err := time.ParseDuration(raw)
		require.NoError(t, err, "invalid %s", localNodeSoakReportIntervalEnv)
		require.Greater(t, reportInterval, time.Duration(0), "%s must be positive", localNodeSoakReportIntervalEnv)
		cfg.reportInterval = reportInterval
	}
	if raw := os.Getenv(localNodeSoakLoopPauseEnv); raw != "" {
		loopPause, err := time.ParseDuration(raw)
		require.NoError(t, err, "invalid %s", localNodeSoakLoopPauseEnv)
		require.GreaterOrEqual(t, loopPause, time.Duration(0), "%s must be non-negative", localNodeSoakLoopPauseEnv)
		cfg.loopPause = loopPause
	}
	if raw := os.Getenv(localNodeSoakBatchSizeEnv); raw != "" {
		batchSize, err := strconv.Atoi(raw)
		require.NoError(t, err, "invalid %s", localNodeSoakBatchSizeEnv)
		require.Greater(t, batchSize, 0, "%s must be positive", localNodeSoakBatchSizeEnv)
		cfg.batchSize = batchSize
	}

	return cfg
}

func bytesToMiB(bytes uint64) float64 {
	return float64(bytes) / (1024 * 1024)
}

func bytesToMiBDelta(current, baseline uint64) float64 {
	if current >= baseline {
		return bytesToMiB(current - baseline)
	}
	return -bytesToMiB(baseline - current)
}

func Example_localNodeSoakCommand() {
	fmt.Println("$env:STREAMKIT_NODE_SOAK='1'; go test ./pkg/server -run TestShouldLocalNodeManagerRetentionSoak -count=1 -v")
	// Output:
	// $env:STREAMKIT_NODE_SOAK='1'; go test ./pkg/server -run TestShouldLocalNodeManagerRetentionSoak -count=1 -v
}
