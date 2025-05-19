package node

import (
	"context"
	"sync"

	"github.com/fgrzl/streamkit/pkg/storage"
)

// StoreFactory defines how to create new storage instances by name.
type StoreFactory interface {
	NewStore(ctx context.Context, store string) (storage.Store, error)
}

// NodeManager manages per-store storage instances.
type NodeManager interface {
	GetOrCreate(ctx context.Context, store string) (storage.Store, error)
	Remove(ctx context.Context, store string)
}

type nodeManager struct {
	mu      sync.RWMutex
	factory StoreFactory
	stores  map[string]storage.Store
}

// NewNodeManager creates a new NodeManager with the given factory.
func NewNodeManager(factory StoreFactory) NodeManager {
	return &nodeManager{
		factory: factory,
		stores:  make(map[string]storage.Store),
	}
}

func (n *nodeManager) GetOrCreate(ctx context.Context, store string) (storage.Store, error) {
	n.mu.RLock()
	s, ok := n.stores[store]
	n.mu.RUnlock()
	if ok {
		return s, nil
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Double-check after acquiring write lock.
	if s, ok := n.stores[store]; ok {
		return s, nil
	}

	storeInstance, err := n.factory.NewStore(ctx, store)
	if err != nil {
		return nil, err
	}

	n.stores[store] = storeInstance
	return storeInstance, nil
}

func (n *nodeManager) Remove(ctx context.Context, store string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if s, ok := n.stores[store]; ok {
		_ = s.Close() // ignore error, optionally log it
		delete(n.stores, store)
	}
}
