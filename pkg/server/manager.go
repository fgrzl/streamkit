package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/fgrzl/messaging"
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
func WithMessageBusFactory(busFactory messaging.MessageBusFactory) NodeManagerOption {
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
	busFactory    messaging.MessageBusFactory
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

func (m *nodeManager) GetOrCreate(ctx context.Context, storeID uuid.UUID) (Node, error) {
	m.mu.RLock()
	s, ok := m.nodes[storeID]
	m.mu.RUnlock()
	if ok {
		return s, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock.
	if s, ok := m.nodes[storeID]; ok {
		return s, nil
	}

	// Check circuit breaker: if store has failed too many times recently, fail fast
	if failure, exists := m.failures[storeID]; exists {
		if failure.count >= m.maxFailures {
			// Circuit is open - check if enough time has passed to retry
			if time.Since(failure.lastFailed) < m.failureWindow {
				return nil, fmt.Errorf("store creation circuit open: too many recent failures (%d), retry after %v",
					failure.count, m.failureWindow-time.Since(failure.lastFailed))
			}
			// Enough time has passed, allow retry (half-open state)
			failure.count = m.maxFailures - 1 // Reduce count to allow one retry
		}
	}

	store, err := m.storeFactory.NewStore(ctx, storeID)
	if err != nil {
		// Track failure
		if failure, exists := m.failures[storeID]; exists {
			failure.count++
			failure.lastFailed = time.Now()
		} else {
			m.failures[storeID] = &storeFailure{count: 1, lastFailed: time.Now()}
		}
		return nil, err
	}

	// Success - clear failures
	delete(m.failures, storeID)

	node := NewNode(storeID, store, m.busFactory)
	m.nodes[storeID] = node
	return node, nil
}

func (m *nodeManager) Remove(ctx context.Context, storeID uuid.UUID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if node, ok := m.nodes[storeID]; ok {
		node.Close()
		delete(m.nodes, storeID)
	}
}

func (m *nodeManager) Close() {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for _, node := range m.nodes {
			node.Close()
		}
	})
}
