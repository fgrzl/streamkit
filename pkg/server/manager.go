package server

import (
	"context"
	"sync"

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

type nodeManager struct {
	mu           sync.RWMutex
	busFactory   messaging.MessageBusFactory
	storeFactory storage.StoreFactory
	nodes        map[uuid.UUID]Node
	closeOnce    sync.Once
}

// NewNodeManager creates a new NodeManager with the specified options.
func NewNodeManager(opts ...NodeManagerOption) NodeManager {
	n := &nodeManager{
		nodes: make(map[uuid.UUID]Node),
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

	store, err := m.storeFactory.NewStore(ctx, storeID)
	if err != nil {
		return nil, err
	}

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
