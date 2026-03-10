package lease

import (
	"sync"
	"time"
)

// leaseState holds the current holder and expiry for a key.
type leaseState struct {
	Holder    string
	ExpiresAt int64
}

// Store is a lightweight in-memory lease store for optimization hints.
// It is not for correctness; multiple workers use it to avoid redundant work on the same key.
type Store struct {
	mu     sync.Mutex
	leases map[string]leaseState
}

// NewStore returns a new lease store.
func NewStore() *Store {
	return &Store{
		leases: make(map[string]leaseState),
	}
}

// Acquire tries to acquire the lease for key. If the key is unowned or expired,
// or the same holder re-acquires, it assigns the holder and sets expiry to now+ttl and returns true.
// If another holder has a valid lease, returns false.
func (s *Store) Acquire(key, holderID string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixNano()
	if state, ok := s.leases[key]; ok && state.Holder != holderID && state.ExpiresAt > now {
		return false
	}
	s.leases[key] = leaseState{Holder: holderID, ExpiresAt: time.Now().Add(ttl).UnixNano()}
	return true
}

// Renew extends the lease for key if the current holder is holderID and the lease is not expired.
// Sets expiry to now+ttl. Returns true on success, false otherwise.
func (s *Store) Renew(key, holderID string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixNano()
	state, ok := s.leases[key]
	if !ok || state.Holder != holderID || state.ExpiresAt <= now {
		return false
	}
	s.leases[key] = leaseState{Holder: holderID, ExpiresAt: time.Now().Add(ttl).UnixNano()}
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
