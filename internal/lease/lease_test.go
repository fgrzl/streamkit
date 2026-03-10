package lease

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAcquireOnce(t *testing.T) {
	s := NewStore()
	ok := s.Acquire("k", "holder1", 30*time.Second)
	assert.True(t, ok)
}

func TestSecondAcquireSameKeyFails(t *testing.T) {
	s := NewStore()
	assert.True(t, s.Acquire("k", "holder1", 30*time.Second))
	ok := s.Acquire("k", "holder2", 30*time.Second)
	assert.False(t, ok)
}

func TestReleaseThenReacquire(t *testing.T) {
	s := NewStore()
	assert.True(t, s.Acquire("k", "holder1", 30*time.Second))
	assert.True(t, s.Release("k", "holder1"))
	ok := s.Acquire("k", "holder2", 30*time.Second)
	assert.True(t, ok)
}

func TestSameHolderReacquireRefreshes(t *testing.T) {
	s := NewStore()
	assert.True(t, s.Acquire("k", "holder1", 10*time.Millisecond))
	ok := s.Acquire("k", "holder1", 30*time.Second)
	assert.True(t, ok)
}

func TestRenewExtendsExpiry(t *testing.T) {
	s := NewStore()
	assert.True(t, s.Acquire("k", "holder1", 50*time.Millisecond))
	time.Sleep(10 * time.Millisecond)
	ok := s.Renew("k", "holder1", 100*time.Millisecond)
	assert.True(t, ok)
	time.Sleep(60 * time.Millisecond)
	// Original would have expired; renewed should still hold
	ok2 := s.Acquire("k", "holder2", 30*time.Second)
	assert.False(t, ok2)
}

func TestRenewByNonHolderFails(t *testing.T) {
	s := NewStore()
	assert.True(t, s.Acquire("k", "holder1", 30*time.Second))
	ok := s.Renew("k", "holder2", 30*time.Second)
	assert.False(t, ok)
}

func TestReleaseByNonHolderFails(t *testing.T) {
	s := NewStore()
	assert.True(t, s.Acquire("k", "holder1", 30*time.Second))
	ok := s.Release("k", "holder2")
	assert.False(t, ok)
	// holder1 still holds
	ok2 := s.Acquire("k", "holder2", 30*time.Second)
	assert.False(t, ok2)
}

func TestExpiredLeaseCanBeReacquired(t *testing.T) {
	s := NewStore()
	assert.True(t, s.Acquire("k", "holder1", 5*time.Millisecond))
	time.Sleep(15 * time.Millisecond)
	ok := s.Acquire("k", "holder2", 30*time.Second)
	assert.True(t, ok)
}
