package cache

import (
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
)

// CacheItem stores the value and expiration time
type CacheItem struct {
	Value      interface{}
	Expiration int64
}

// ExpiringCache struct using sync.Map
type ExpiringCache struct {
	store    sync.Map
	ttl      time.Duration
	interval time.Duration
	stop     chan struct{}
	disposed sync.Once
}

// NewExpiringCache creates a new cache with expiration and cleanup interval
func NewExpiringCache(ttl, cleanupInterval time.Duration) *ExpiringCache {
	cache := &ExpiringCache{
		ttl:      ttl,
		interval: cleanupInterval,
		stop:     make(chan struct{}),
	}

	// Start cleanup goroutine
	go cache.cleanupExpiredEntries()

	return cache
}

// Set inserts a key-value pair with expiration
func (c *ExpiringCache) Set(key string, value interface{}) {
	expiration := time.Now().Add(c.ttl).UnixNano()
	c.store.Store(key, CacheItem{Value: value, Expiration: expiration})
}

// Get retrieves a value, returning nil if expired or not found
func (c *ExpiringCache) Get(key string) (interface{}, bool) {
	item, ok := c.store.Load(key)
	if !ok {
		return nil, false
	}

	cacheItem := item.(CacheItem)
	if time.Now().UnixNano() > cacheItem.Expiration {
		c.store.Delete(key)
		return nil, false
	}

	return cacheItem.Value, true
}

// Delete removes a key manually
func (c *ExpiringCache) Delete(key uuid.UUID) {
	c.store.Delete(key)
}

// cleanupExpiredEntries runs periodically to remove expired items
func (c *ExpiringCache) cleanupExpiredEntries() {

	runtime.Gosched()

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now().UnixNano()
			c.store.Range(func(key, value any) bool {
				if value.(CacheItem).Expiration < now {
					c.store.Delete(key)
				}
				return true
			})
		case <-c.stop:
			return
		}
	}
}

// Stop stops the cleanup goroutine
func (c *ExpiringCache) Close() {
	c.disposed.Do(func() {
		close(c.stop)
	})
}
