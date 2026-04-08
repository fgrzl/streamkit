package cache

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *captureHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, record.Clone())
	return nil
}

func (h *captureHandler) WithAttrs(_ []slog.Attr) slog.Handler {
	return h
}

func (h *captureHandler) WithGroup(_ string) slog.Handler {
	return h
}

func (h *captureHandler) Records() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()

	records := make([]slog.Record, len(h.records))
	copy(records, h.records)
	return records
}

func recordAttrs(record slog.Record) map[string]string {
	attrs := make(map[string]string)
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.String()
		return true
	})
	return attrs
}

func setDefaultLogger(t *testing.T, handler slog.Handler) {
	t.Helper()

	previous := slog.Default()
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() {
		slog.SetDefault(previous)
	})
}

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
	assert.Equal(t, int32(1), c.CleanupPanicCount())
}

func TestShouldLogRecoveredCleanupPanic(t *testing.T) {
	// Arrange
	handler := &captureHandler{}
	setDefaultLogger(t, handler)

	c := NewExpiringCache(30*time.Millisecond, 10*time.Millisecond)
	defer c.Close()

	var ticks atomic.Int32
	c.cleanupTickTestHook.Store(func() {
		if ticks.Add(1) == 1 {
			panic("cleanup panic")
		}
	})

	// Act
	require.Eventually(t, func() bool {
		return len(handler.Records()) >= 1
	}, time.Second, 10*time.Millisecond)

	// Assert
	records := handler.Records()
	require.Len(t, records, 1)
	assert.Equal(t, "cache: cleanup panic recovered; restarting cleanup loop", records[0].Message)

	attrs := recordAttrs(records[0])
	assert.Equal(t, "1", attrs["cleanup_panic_count"])
	assert.Equal(t, "5", attrs["max_cleanup_panics"])
	assert.Equal(t, "string", attrs["panic_type"])
	assert.Equal(t, "cleanup panic", attrs["panic_value"])
	assert.Contains(t, attrs["stack"], "cleanupExpiredEntries")
}

func TestShouldLogAndStopCleanupLoopAfterMaxPanicsExceeded(t *testing.T) {
	// Arrange
	handler := &captureHandler{}
	setDefaultLogger(t, handler)

	c := NewExpiringCache(30*time.Millisecond, 10*time.Millisecond)
	c.maxCleanupPanics = 1
	defer c.Close()

	c.cleanupTickTestHook.Store(func() {
		panic("cleanup panic")
	})

	// Act
	require.Eventually(t, func() bool {
		return c.CleanupPanicCount() == 2 && len(handler.Records()) >= 2
	}, time.Second, 10*time.Millisecond)
	time.Sleep(40 * time.Millisecond)

	// Assert
	records := handler.Records()
	require.Len(t, records, 2)
	assert.Equal(t, "cache: cleanup panic recovered; restarting cleanup loop", records[0].Message)
	assert.Equal(t, "cache: cleanup panic limit exceeded; cleanup loop stopped", records[1].Message)
	assert.Equal(t, int32(2), c.CleanupPanicCount())

	attrs := recordAttrs(records[1])
	assert.Equal(t, "2", attrs["cleanup_panic_count"])
	assert.Equal(t, "1", attrs["max_cleanup_panics"])
	assert.Equal(t, "cleanup panic", attrs["panic_value"])
	assert.Contains(t, attrs["stack"], "cleanupExpiredEntries")
}
