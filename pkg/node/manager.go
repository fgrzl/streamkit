package node

import (
	"context"
	"sync"

	"github.com/fgrzl/messaging"
	"github.com/fgrzl/streamkit/pkg/storage"
	"github.com/google/uuid"
)

// NodeManager manages per-store storage instances.
type NodeManager interface {
	GetOrCreate(ctx context.Context, storeID uuid.UUID) (Node, error)
	Remove(ctx context.Context, storeID uuid.UUID)
	Close()
}

type NodeManagerOption func(*nodeManager)

func WithMessageBusFactory(busFactory messaging.MessageBusFactory) NodeManagerOption {
	return func(n *nodeManager) {
		n.busFactory = busFactory
	}
}

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
		node.Close() // ignore error, optionally log it
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
