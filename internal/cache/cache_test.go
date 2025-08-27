package cache_test

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/fgrzl/streamkit/internal/cache"
	"github.com/stretchr/testify/assert"
)

func TestSetGet(t *testing.T) {
	t.Run("Given a cache with short TTL", func(t *testing.T) {
		c := cache.NewExpiringCache(500*time.Millisecond, 100*time.Millisecond)
		defer c.Close()

		t.Run("When a key is set and retrieved", func(t *testing.T) {
			c.Set("k1", "v1")
			v, ok := c.Get("k1")
			assert.True(t, ok, "expected key present")
			assert.Equal(t, "v1", v.(string))
		})
	})
}

func TestExpiration(t *testing.T) {
	t.Run("Given a cache with very short TTL", func(t *testing.T) {
		c := cache.NewExpiringCache(100*time.Millisecond, 50*time.Millisecond)
		defer c.Close()

		t.Run("When a key expires", func(t *testing.T) {
			c.Set("k2", "v2")
			time.Sleep(250 * time.Millisecond)

			_, ok := c.Get("k2")
			assert.False(t, ok, "expected key to be expired")
		})
	})
}

func TestConcurrentSetGet(t *testing.T) {
	c := cache.NewExpiringCache(1*time.Second, 100*time.Millisecond)
	defer c.Close()

	var wg sync.WaitGroup
	n := 100

	t.Run("When many goroutines set and get keys concurrently", func(t *testing.T) {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				key := "k" + strconv.Itoa(i)
				c.Set(key, i)
				if v, ok := c.Get(key); ok {
					_ = v
				}
			}(i)
		}

		wg.Wait()
	})
}
