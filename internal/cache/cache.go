package cache

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
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

	cleanupPanics       atomic.Int32
	maxCleanupPanics    int32
	cleanupTickTestHook atomic.Value // stores func() for test-only fault injection
}

// NewExpiringCache creates a new cache with expiration and cleanup interval
func NewExpiringCache(ttl, cleanupInterval time.Duration) *ExpiringCache {
	cache := &ExpiringCache{
		ttl:              ttl,
		interval:         cleanupInterval,
		stop:             make(chan struct{}),
		maxCleanupPanics: 5,
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

	cacheItem, ok := item.(CacheItem)
	if !ok {
		c.store.Delete(key)
		return nil, false
	}
	if time.Now().UnixNano() > cacheItem.Expiration {
		c.store.Delete(key)
		return nil, false
	}

	return cacheItem.Value, true
}

// Delete removes a key manually
func (c *ExpiringCache) Delete(key string) {
	c.store.Delete(key)
}

// cleanupExpiredEntries runs periodically to remove expired items
func (c *ExpiringCache) cleanupExpiredEntries() {
	defer func() {
		if recover() != nil {
			if c.cleanupPanics.Add(1) <= c.maxCleanupPanics {
				select {
				case <-c.stop:
					return
				default:
				}
				go c.cleanupExpiredEntries()
			}
		}
	}()

	runtime.Gosched()

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if hookAny := c.cleanupTickTestHook.Load(); hookAny != nil {
				if hook, ok := hookAny.(func()); ok && hook != nil {
					hook()
				}
			}
			now := time.Now().UnixNano()
			c.store.Range(func(key, value any) bool {
				if item, ok := value.(CacheItem); ok {
					if item.Expiration < now {
						c.store.Delete(key)
					}
				} else {
					// Remove malformed entries
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
