package pebblekit

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldReturnSameMutexForSameSegment(t *testing.T) {
	store := newTestPebbleStore(t)
	a := store.getSegmentLock("space", "seg")
	b := store.getSegmentLock("space", "seg")
	assert.True(t, a == b)
}

func TestShouldNotGrowSegLocksBeyondCap(t *testing.T) {
	store := newTestPebbleStore(t)
	cap := 16
	store.maxSegLocks = cap
	n := cap + cap/2
	for i := range n {
		_ = store.getSegmentLock("sp", fmt.Sprintf("seg%d", i))
	}
	// May slightly exceed cap only if every mutex were held during eviction (not the case here).
	assert.LessOrEqual(t, store.SegLocksSize(), cap+cap/10+2)
}

func TestShouldNotEvictHeldSegLock(t *testing.T) {
	store := newTestPebbleStore(t)
	store.maxSegLocks = 8
	held := store.getSegmentLock("held", "x")
	held.Lock()
	for i := range 50 {
		_ = store.getSegmentLock("sp", fmt.Sprintf("s%d", i))
	}
	mu2 := store.getSegmentLock("held", "x")
	assert.Same(t, held, mu2)
	held.Unlock()
}

func TestShouldEvictOnlyUnheldSegLocksConcurrently(t *testing.T) {
	store := newTestPebbleStore(t)
	store.maxSegLocks = 64
	var wg sync.WaitGroup
	for g := range 20 {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := range 100 {
				mu := store.getSegmentLock("sp", fmt.Sprintf("%d-%d", base, i))
				mu.Lock()
				runtime.Gosched() // held lock: eviction uses TryLock on other entries
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	assert.LessOrEqual(t, store.SegLocksSize(), 80)
}
