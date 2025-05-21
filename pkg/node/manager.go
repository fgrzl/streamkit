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

type nodeManager struct {
	mu        sync.RWMutex
	bus       messaging.MessageBus
	factory   storage.StoreFactory
	nodes     map[uuid.UUID]Node
	closeOnce sync.Once
}

// NewNodeManager creates a new NodeManager with the given factory.
func NewNodeManager(bus messaging.MessageBus, factory storage.StoreFactory) NodeManager {
	return &nodeManager{
		bus:     bus,
		factory: factory,
		nodes:   make(map[uuid.UUID]Node),
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

	// Double-check after acquiring write lock.
	if s, ok := m.nodes[storeID]; ok {
		return s, nil
	}

	storeInstance, err := m.factory.NewStore(ctx, storeID)
	if err != nil {
		return nil, err
	}

	node := NewNode(storeID, storeInstance, m.bus)
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
