package pebblekit

import (
	"context"
	"path/filepath"
	"time"

	"github.com/fgrzl/streamkit/internal/cache"
	"github.com/fgrzl/streamkit/pkg/storage"
	"github.com/google/uuid"
)

var (
	CacheTTL             time.Duration = time.Second * 97
	CacheCleanupInterval time.Duration = time.Second * 59
)

// AzureStoreOptions configures the Azure Table Storage client.
type PebbleStoreOptions struct {
	Path string
}

// StoreFactory creates Azure-backed stores using shared credentials.
type StoreFactory struct {
	options *PebbleStoreOptions
}

// NewStoreFactory validates options and initializes credentials.
func NewStoreFactory(options *PebbleStoreOptions) (*StoreFactory, error) {
	f := &StoreFactory{options: options}
	return f, nil
}

func (f *StoreFactory) NewStore(ctx context.Context, storeID uuid.UUID) (storage.Store, error) {
	path := filepath.Join(f.options.Path, storeID.String())
	cache := cache.NewExpiringCache(CacheTTL, CacheCleanupInterval)
	return NewPebbleStore(path, cache)
}
