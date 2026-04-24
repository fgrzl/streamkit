package server

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fgrzl/streamkit/internal/lease"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/bus"
	"github.com/fgrzl/streamkit/pkg/storage"
	"github.com/google/uuid"
)

const defaultMaxFailureEntries = 1024

// NodeManager manages per-store storage instances and their lifecycle.
type NodeManager interface {
	// GetOrCreate retrieves an existing Node or creates a new one for the given store ID.
	GetOrCreate(ctx context.Context, storeID uuid.UUID) (Node, error)
	// Remove closes and removes the Node associated with the given store ID.
	Remove(ctx context.Context, storeID uuid.UUID)
	// Close shuts down all managed nodes and releases resources.
	Close()
}

// NodeManagerOption defines functional options for configuring a NodeManager.
type NodeManagerOption func(*nodeManager)

// WithMessageBusFactory configures the NodeManager to use the specified message bus factory.
func WithMessageBusFactory(busFactory bus.MessageBusFactory) NodeManagerOption {
	return func(n *nodeManager) {
		n.busFactory = busFactory
	}
}

// WithStoreFactory configures the NodeManager to use the specified store factory.
func WithStoreFactory(storeFactory storage.StoreFactory) NodeManagerOption {
	return func(n *nodeManager) {
		n.storeFactory = storeFactory
	}
}

// WithIdleEviction enables a background reaper that closes and removes nodes that have
// had no GetOrCreate traffic and no in-flight Handle calls for idleTTL.
// Both idleTTL and checkInterval must be positive; otherwise eviction stays disabled.
func WithIdleEviction(idleTTL, checkInterval time.Duration) NodeManagerOption {
	return func(n *nodeManager) {
		if idleTTL > 0 && checkInterval > 0 {
			n.idleTTL = idleTTL
			n.idleCheckEvery = checkInterval
		}
	}
}

type storeFailure struct {
	count      int
	lastFailed time.Time
}

type nodeEntry struct {
	inner        Node
	lastAccessed atomic.Int64
	inflight     atomic.Int32
}

type refCountedNode struct {
	m       *nodeManager
	storeID uuid.UUID
	entry   *nodeEntry
}

func (w *refCountedNode) Handle(ctx context.Context, bidi api.BidiStream) {
	w.entry.inflight.Add(1)
	defer w.entry.inflight.Add(-1)
	w.entry.lastAccessed.Store(time.Now().UnixNano())
	w.entry.inner.Handle(ctx, bidi)
}

func (w *refCountedNode) Close() {
	w.entry.inner.Close()
}

type nodeManager struct {
	mu                sync.RWMutex
	busFactory        bus.MessageBusFactory
	storeFactory      storage.StoreFactory
	nodes             map[uuid.UUID]*nodeEntry
	failures          map[uuid.UUID]*storeFailure
	closeOnce         sync.Once
	failureWindow     time.Duration // How long to wait before retrying after failures
	maxFailures       int           // Circuit opens after this many consecutive failures
	maxFailureEntries int           // cap on distinct store IDs tracked in failures

	idleTTL        time.Duration
	idleCheckEvery time.Duration
	reaperStop     chan struct{}
	reaperWG       sync.WaitGroup
}

// NewNodeManager creates a new NodeManager with the specified options.
func NewNodeManager(opts ...NodeManagerOption) NodeManager {
	n := &nodeManager{
		nodes:             make(map[uuid.UUID]*nodeEntry),
		failures:          make(map[uuid.UUID]*storeFailure),
		failureWindow:     30 * time.Second, // Default: wait 30s after 3 consecutive failures
		maxFailures:       3,                // Default: circuit opens after 3 failures
		maxFailureEntries: defaultMaxFailureEntries,
	}
	for _, opt := range opts {
		opt(n)
	}
	if n.idleTTL > 0 && n.idleCheckEvery > 0 {
		n.reaperStop = make(chan struct{})
		n.reaperWG.Add(1)
		go func() {
			defer n.reaperWG.Done()
			n.idleReaper()
		}()
	}
	return n
}

func (m *nodeManager) pruneExpiredFailuresLocked(now time.Time) {
	if len(m.failures) == 0 || m.failureWindow <= 0 {
		return
	}
	for storeID, failure := range m.failures {
		if failure == nil || now.Sub(failure.lastFailed) >= m.failureWindow {
			delete(m.failures, storeID)
		}
	}
}

func (m *nodeManager) GetOrCreate(ctx context.Context, storeID uuid.UUID) (Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneExpiredFailuresLocked(time.Now())

	if ent, ok := m.nodes[storeID]; ok {
		ent.lastAccessed.Store(time.Now().UnixNano())
		return &refCountedNode{m: m, storeID: storeID, entry: ent}, nil
	}

	// Check circuit breaker: if store has failed too many times recently, fail fast
	if failure, exists := m.failures[storeID]; exists {
		if failure.count >= m.maxFailures {
			// Circuit is open - check if enough time has passed to retry
			if time.Since(failure.lastFailed) < m.failureWindow {
				retryAfter := m.failureWindow - time.Since(failure.lastFailed)
				if retryAfter < 0 {
					retryAfter = 0
				}
				slog.WarnContext(ctx, "node manager: store creation circuit open",
					slog.String("store_id", storeID.String()),
					slog.Int("failure_count", failure.count),
					slog.Duration("retry_after", retryAfter),
					slog.Duration("failure_window", m.failureWindow))
				return nil, fmt.Errorf("store creation circuit open: too many recent failures (%d), retry after %v",
					failure.count, m.failureWindow-time.Since(failure.lastFailed))
			}
			// Enough time has passed, allow retry (half-open state)
			slog.InfoContext(ctx, "node manager: retrying store creation after failure window",
				slog.String("store_id", storeID.String()),
				slog.Int("failure_count", failure.count),
				slog.Duration("failure_window", m.failureWindow))
			failure.count = m.maxFailures - 1 // Reduce count to allow one retry
		}
	}

	store, err := m.storeFactory.NewStore(ctx, storeID)
	if err != nil {
		// Track failure
		failureCount := 1
		if failure, exists := m.failures[storeID]; exists {
			failure.count++
			failure.lastFailed = time.Now()
			failureCount = failure.count
		} else {
			if len(m.failures) >= m.maxFailureEntries {
				m.evictOldestFailureLocked()
			}
			m.failures[storeID] = &storeFailure{count: 1, lastFailed: time.Now()}
		}
		slog.ErrorContext(ctx, "node manager: store creation failed",
			slog.String("store_id", storeID.String()),
			slog.Int("failure_count", failureCount),
			slog.Int("max_failures", m.maxFailures),
			slog.Bool("circuit_open", failureCount >= m.maxFailures),
			slog.String("error", err.Error()))
		return nil, err
	}

	// Success - clear failures
	previousFailures := 0
	if failure, exists := m.failures[storeID]; exists {
		previousFailures = failure.count
	}
	delete(m.failures, storeID)
	if previousFailures > 0 {
		slog.InfoContext(ctx, "node manager: store creation recovered",
			slog.String("store_id", storeID.String()),
			slog.Int("previous_failure_count", previousFailures))
	}

	leaseStore := lease.NewStore()
	inner := NewNode(storeID, store, m.busFactory, leaseStore)
	newEnt := &nodeEntry{inner: inner}
	newEnt.lastAccessed.Store(time.Now().UnixNano())
	m.nodes[storeID] = newEnt
	return &refCountedNode{m: m, storeID: storeID, entry: newEnt}, nil
}

func (m *nodeManager) Remove(ctx context.Context, storeID uuid.UUID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.failures, storeID)

	if ent, ok := m.nodes[storeID]; ok {
		ent.inner.Close()
		delete(m.nodes, storeID)
	}
}

func (m *nodeManager) Close() {
	m.closeOnce.Do(func() {
		if m.reaperStop != nil {
			close(m.reaperStop)
			m.reaperWG.Wait()
		}
		m.mu.Lock()
		toClose := make([]Node, 0, len(m.nodes))
		for _, ent := range m.nodes {
			toClose = append(toClose, ent.inner)
		}
		m.nodes = make(map[uuid.UUID]*nodeEntry)
		m.failures = make(map[uuid.UUID]*storeFailure)
		m.mu.Unlock()

		for _, node := range toClose {
			node.Close()
		}
	})
}

func (m *nodeManager) idleReaper() {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("node manager: idle reaper panic recovered",
				slog.Any("panic", r),
				slog.String("stack", string(debug.Stack())))
		}
	}()
	ticker := time.NewTicker(m.idleCheckEvery)
	defer ticker.Stop()
	for {
		select {
		case <-m.reaperStop:
			return
		case <-ticker.C:
			m.reapIdleNodes()
		}
	}
}

func (m *nodeManager) reapIdleNodes() {
	if m.idleTTL <= 0 {
		return
	}
	now := time.Now()
	m.mu.Lock()
	var toClose []Node
	for sid, ent := range m.nodes {
		if ent.inflight.Load() != 0 {
			continue
		}
		if now.Sub(time.Unix(0, ent.lastAccessed.Load())) < m.idleTTL {
			continue
		}
		toClose = append(toClose, ent.inner)
		delete(m.nodes, sid)
	}
	m.mu.Unlock()
	for _, inner := range toClose {
		inner.Close()
	}
}

// evictOldestFailureLocked removes one failure entry with the smallest lastFailed.
// Caller must hold m.mu (write lock).
func (m *nodeManager) evictOldestFailureLocked() {
	if len(m.failures) == 0 {
		return
	}
	var victim uuid.UUID
	var oldest time.Time
	first := true
	for id, f := range m.failures {
		if first || f.lastFailed.Before(oldest) {
			first = false
			oldest = f.lastFailed
			victim = id
		}
	}
	if !first {
		delete(m.failures, victim)
	}
}

// Compile-time check that refCountedNode implements Node.
var _ Node = (*refCountedNode)(nil)
