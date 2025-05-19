package streamkit

import (
	"sync"

	"github.com/fgrzl/streamkit/pkg/api"
)

type TenantClientPool struct {
	mu      sync.RWMutex
	clients map[string]Client
	factory func(tenantID string) api.BidiStreamProvider
}

func NewTenantClientPool(factory func(tenantID string) api.BidiStreamProvider) *TenantClientPool {
	return &TenantClientPool{
		clients: make(map[string]Client),
		factory: factory,
	}
}

func (p *TenantClientPool) GetClient(tenantID string) Client {
	p.mu.RLock()
	client, ok := p.clients[tenantID]
	p.mu.RUnlock()
	if ok {
		return client
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Re-check in case it was created between locks
	if client, ok := p.clients[tenantID]; ok {
		return client
	}

	provider := p.factory(tenantID)
	client = NewClient(provider)
	p.clients[tenantID] = client
	return client
}
