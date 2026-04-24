package lease

import (
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// leaseState holds the current holder and expiry for a key.
type leaseState struct {
	Holder    string
	ExpiresAt int64
}

// Option configures a Store.
type Option func(*Store)

// WithCleanupInterval sets how often the background goroutine removes expired
// leases. Zero or negative disables the sweeper (no background goroutine).
func WithCleanupInterval(d time.Duration) Option {
	return func(s *Store) {
		s.cleanupInterval = d
	}
}

// Store is a lightweight in-memory lease store for optimization hints.
// It is not for correctness; multiple workers use it to avoid redundant work on the same key.
type Store struct {
	mu               sync.Mutex
	leases           map[string]leaseState
	cleanupInterval  time.Duration
	stop             chan struct{}
	disposed         sync.Once
	cleanupPanics    atomic.Int32
	maxCleanupPanics int32
}

const defaultLeaseCleanupInterval = 60 * time.Second

// NewStore returns a new lease store. Options may include WithCleanupInterval.
func NewStore(opts ...Option) *Store {
	s := &Store{
		leases:           make(map[string]leaseState),
		cleanupInterval:  defaultLeaseCleanupInterval,
		stop:             make(chan struct{}),
		maxCleanupPanics: 5,
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.cleanupInterval > 0 {
		go s.cleanupExpiredLeases()
	}
	return s
}

// Size returns the number of lease entries (for tests and diagnostics).
func (s *Store) Size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.leases)
}

// Close stops the background cleanup goroutine. Idempotent.
func (s *Store) Close() {
	s.disposed.Do(func() {
		close(s.stop)
	})
}

// CleanupPanicCount returns recovered panics in the cleanup loop (tests).
func (s *Store) CleanupPanicCount() int32 {
	return s.cleanupPanics.Load()
}

func (s *Store) removeExpiredLocked(now int64) {
	for k, st := range s.leases {
		if st.ExpiresAt <= now {
			delete(s.leases, k)
		}
	}
}

// Acquire tries to acquire the lease for key. If the key is unowned or expired,
// or the same holder re-acquires, it assigns the holder and sets expiry to now+ttl and returns true.
// If another holder has a valid lease, returns false.
func (s *Store) Acquire(key, holderID string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixNano()
	s.removeExpiredLocked(now)
	if state, ok := s.leases[key]; ok && state.Holder != holderID && state.ExpiresAt > now {
		return false
	}
	expiresAt := now + int64(ttl)
	s.leases[key] = leaseState{Holder: holderID, ExpiresAt: expiresAt}
	return true
}

// Renew extends the lease for key if the current holder is holderID and the lease is not expired.
// Sets expiry to now+ttl. Returns true on success, false otherwise.
func (s *Store) Renew(key, holderID string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixNano()
	s.removeExpiredLocked(now)
	state, ok := s.leases[key]
	if !ok || state.Holder != holderID || state.ExpiresAt <= now {
		return false
	}
	expiresAt := now + int64(ttl)
	s.leases[key] = leaseState{Holder: holderID, ExpiresAt: expiresAt}
	return true
}

// Release releases the lease for key if the current holder is holderID.
// Returns true if the lease was released, false otherwise.
func (s *Store) Release(key, holderID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.leases[key]
	if !ok || state.Holder != holderID {
		return false
	}
	delete(s.leases, key)
	return true
}

func (s *Store) cleanupExpiredLeases() {
	defer s.recoverCleanupPanic()

	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now().UnixNano()
			s.mu.Lock()
			s.removeExpiredLocked(now)
			s.mu.Unlock()
		case <-s.stop:
			return
		}
	}
}

func (s *Store) recoverCleanupPanic() {
	recovered := recover()
	if recovered == nil {
		return
	}
	panicCount := s.cleanupPanics.Add(1)
	willRestart := panicCount <= s.maxCleanupPanics
	fields := []any{
		slog.Int("cleanup_panic_count", int(panicCount)),
		slog.Int("max_cleanup_panics", int(s.maxCleanupPanics)),
		slog.String("panic_type", fmt.Sprintf("%T", recovered)),
		slog.String("panic_value", fmt.Sprint(recovered)),
		slog.String("stack", string(debug.Stack())),
	}
	if willRestart {
		slog.Error("lease: cleanup panic recovered; restarting cleanup loop", fields...)
		select {
		case <-s.stop:
			return
		default:
		}
		go s.cleanupExpiredLeases()
		return
	}
	slog.Error("lease: cleanup panic limit exceeded; cleanup loop stopped", fields...)
}
