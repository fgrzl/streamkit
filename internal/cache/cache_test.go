package cache

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldSetAndGetValueFromCache(t *testing.T) {
	// Arrange
	c := NewExpiringCache(500*time.Millisecond, 100*time.Millisecond)
	defer c.Close()

	// Act
	c.Set("k1", "v1")
	v, ok := c.Get("k1")

	// Assert
	require.True(t, ok, "expected key present")
	assert.Equal(t, "v1", v.(string))
}

func TestShouldExpireKeyAfterTTL(t *testing.T) {
	// Arrange
	c := NewExpiringCache(100*time.Millisecond, 50*time.Millisecond)
	defer c.Close()

	// Act
	c.Set("k2", "v2")
	time.Sleep(250 * time.Millisecond)

	// Assert
	_, ok := c.Get("k2")
	assert.False(t, ok, "expected key to be expired")
}

func TestShouldSupportConcurrentSetAndGet(t *testing.T) {
	// Arrange
	c := NewExpiringCache(1*time.Second, 100*time.Millisecond)
	defer c.Close()

	var wg sync.WaitGroup
	n := 100

	// Act
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

	// Assert
	// If we reach here without race or panics the concurrent access passed.
	assert.True(t, true)
}

func TestShouldRecoverCleanupLoopFromPanic(t *testing.T) {
	// Arrange
	c := NewExpiringCache(30*time.Millisecond, 10*time.Millisecond)
	defer c.Close()

	var ticks atomic.Int32
	c.cleanupTickTestHook.Store(func() {
		if ticks.Add(1) == 1 {
			panic("cleanup panic")
		}
	})

	c.Set("k-panic", "v")

	// Act + Assert: cleanup loop should recover and continue expiring entries.
	require.Eventually(t, func() bool {
		_, ok := c.Get("k-panic")
		return !ok
	}, time.Second, 10*time.Millisecond)
	require.GreaterOrEqual(t, ticks.Load(), int32(2))
}
