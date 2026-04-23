package server

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/fgrzl/streamkit/internal/lease"
	"github.com/fgrzl/streamkit/pkg/bus"
	"github.com/fgrzl/streamkit/pkg/storage"
	"github.com/google/uuid"
)

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

type storeFailure struct {
	count      int
	lastFailed time.Time
}

type nodeManager struct {
	mu            sync.RWMutex
	busFactory    bus.MessageBusFactory
	storeFactory  storage.StoreFactory
	nodes         map[uuid.UUID]Node
	failures      map[uuid.UUID]*storeFailure
	closeOnce     sync.Once
	failureWindow time.Duration // How long to wait before retrying after failures
	maxFailures   int           // Circuit opens after this many consecutive failures
}

// NewNodeManager creates a new NodeManager with the specified options.
func NewNodeManager(opts ...NodeManagerOption) NodeManager {
	n := &nodeManager{
		nodes:         make(map[uuid.UUID]Node),
		failures:      make(map[uuid.UUID]*storeFailure),
		failureWindow: 30 * time.Second, // Default: wait 30s after 3 consecutive failures
		maxFailures:   3,                // Default: circuit opens after 3 failures
	}
	for _, opt := range opts {
		opt(n)
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
	m.mu.RLock()
	s, ok := m.nodes[storeID]
	m.mu.RUnlock()
	if ok {
		return s, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneExpiredFailuresLocked(time.Now())

	// Double-check after acquiring write lock.
	if s, ok := m.nodes[storeID]; ok {
		return s, nil
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
	node := NewNode(storeID, store, m.busFactory, leaseStore)
	m.nodes[storeID] = node
	return node, nil
}

func (m *nodeManager) Remove(ctx context.Context, storeID uuid.UUID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.failures, storeID)

	if node, ok := m.nodes[storeID]; ok {
		node.Close()
		delete(m.nodes, storeID)
	}
}

func (m *nodeManager) Close() {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		nodes := make([]Node, 0, len(m.nodes))
		for _, node := range m.nodes {
			nodes = append(nodes, node)
		}
		m.nodes = make(map[uuid.UUID]Node)
		m.failures = make(map[uuid.UUID]*storeFailure)
		m.mu.Unlock()

		for _, node := range nodes {
			node.Close()
		}
	})
}
