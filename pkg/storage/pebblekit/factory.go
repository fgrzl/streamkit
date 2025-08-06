// Package pebblekit provides a PebbleDB-based storage implementation
// for the streamkit streaming platform.
//
// This package implements the storage interface using PebbleDB as the
// underlying key-value store, providing high-performance local storage
// with support for transactions, iteration, and persistence.
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
	// CacheTTL defines the time-to-live for cached entries.
	CacheTTL time.Duration = time.Second * 97
	// CacheCleanupInterval defines how often expired cache entries are removed.
	CacheCleanupInterval time.Duration = time.Second * 59
)

// PebbleStoreOptions configures the PebbleDB storage implementation.
type PebbleStoreOptions struct {
	Path string
}

// StoreFactory creates PebbleDB-backed stores with shared configuration.
type StoreFactory struct {
	options *PebbleStoreOptions
}

// NewStoreFactory validates options and creates a new StoreFactory.
func NewStoreFactory(options *PebbleStoreOptions) (*StoreFactory, error) {
	f := &StoreFactory{options: options}
	return f, nil
}

// NewStore creates a new PebbleDB store instance for the given store ID.
func (f *StoreFactory) NewStore(ctx context.Context, storeID uuid.UUID) (storage.Store, error) {
	path := filepath.Join(f.options.Path, storeID.String())
	cache := cache.NewExpiringCache(CacheTTL, CacheCleanupInterval)
	return NewPebbleStore(path, cache)
}
