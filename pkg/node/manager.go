package node

import (
	"context"
	"sync"

	"github.com/fgrzl/streamkit/pkg/storage"
)

// NodeManager manages per-store storage instances.
type NodeManager interface {
	GetOrCreate(ctx context.Context, name string) (Node, error)
	Remove(ctx context.Context, name string)
	Close()
}

type nodeManager struct {
	mu        sync.RWMutex
	factory   storage.StoreFactory
	nodes     map[string]Node
	closeOnce sync.Once
}

// NewNodeManager creates a new NodeManager with the given factory.
func NewNodeManager(factory storage.StoreFactory) NodeManager {
	return &nodeManager{
		factory: factory,
		nodes:   make(map[string]Node),
	}
}

func (m *nodeManager) GetOrCreate(ctx context.Context, store string) (Node, error) {
	m.mu.RLock()
	s, ok := m.nodes[store]
	m.mu.RUnlock()
	if ok {
		return s, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock.
	if s, ok := m.nodes[store]; ok {
		return s, nil
	}

	storeInstance, err := m.factory.NewStore(ctx, store)
	if err != nil {
		return nil, err
	}

	node := NewNode(storeInstance)
	m.nodes[store] = node
	return node, nil
}

func (m *nodeManager) Remove(ctx context.Context, name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if node, ok := m.nodes[name]; ok {
		node.Close() // ignore error, optionally log it
		delete(m.nodes, name)
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
