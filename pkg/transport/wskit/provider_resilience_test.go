package wskit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/websocket"
)

// TestConcurrentCallStreamDuringMuxerReconnect verifies that concurrent CallStream
// operations coordinate properly when the muxer is being replaced during reconnection.
func TestConcurrentCallStreamDuringMuxerReconnect(t *testing.T) {
	// Arrange
	p := NewBidiStreamProvider("https://example.com/", func() (string, error) { return "tok", nil }).(*WebSocketBidiStreamProvider)
	defer p.Close()

	var (
		dialCount      int32
		encodeAttempts int32
		encodeSuccess  int32
	)

	// Create a muxer that fails the first N encode attempts, then succeeds
	const failFirst = 3
	createMuxer := func() *fakeMuxer {
		return &fakeMuxer{
			pingFn: func() bool { return true },
			bidi: &testBidi{
				encodeFn: func(m any) error {
					attempt := atomic.AddInt32(&encodeAttempts, 1)
					if attempt <= failFirst {
						// Simulate muxer closure during encode
						p.mu.Lock()
						p.muxer = nil
						p.mu.Unlock()
						return ErrMuxerClosed
					}
					atomic.AddInt32(&encodeSuccess, 1)
					return nil
				},
			},
		}
	}

	p.dialFn = func() (*websocket.Conn, error) {
		atomic.AddInt32(&dialCount, 1)
		return nil, nil
	}
	p.newClientMuxer = func(ctx context.Context, session MuxerSession, conn *websocket.Conn) providerMuxer {
		return createMuxer()
	}

	// Act: Launch 10 concurrent CallStream operations
	const numGoroutines = 10
	var wg sync.WaitGroup
	errors := make([]error, numGoroutines)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := p.CallStream(ctx, uuid.New(), &api.GetStatus{})
			errors[idx] = err
		}(i)
	}

	wg.Wait()

	// Assert: All operations should eventually succeed through retry logic
	for i, err := range errors {
		assert.NoError(t, err, "goroutine %d should succeed", i)
	}

	// Verify encode attempts show retries happened
	assert.GreaterOrEqual(t, atomic.LoadInt32(&encodeAttempts), int32(failFirst+numGoroutines),
		"expected at least %d encode attempts (%d failures + %d successes)", failFirst+numGoroutines, failFirst, numGoroutines)

	// Verify all goroutines eventually succeeded
	assert.Equal(t, int32(numGoroutines), atomic.LoadInt32(&encodeSuccess),
		"expected all %d goroutines to succeed", numGoroutines)
}

// TestRapidMuxerReplacementDuringHighLoad simulates the scenario where
// the reconnect loop keeps replacing the muxer while operations are in flight.
func TestRapidMuxerReplacementDuringHighLoad(t *testing.T) {
	t.Skip("Stress test - enable manually for extended validation")

	// Arrange
	p := NewBidiStreamProvider("https://example.com/", func() (string, error) { return "tok", nil }).(*WebSocketBidiStreamProvider)
	defer p.Close()

	var (
		muxerCreationCount int32
		successfulEncodes  int32
		failedEncodes      int32
	)

	// Muxer that randomly fails
	createFlakeyMuxer := func() *fakeMuxer {
		creationNum := atomic.AddInt32(&muxerCreationCount, 1)
		return &fakeMuxer{
			pingFn: func() bool { return true },
			bidi: &testBidi{
				encodeFn: func(m any) error {
					// 30% chance of failure simulating network issues
					if creationNum%3 == 0 {
						atomic.AddInt32(&failedEncodes, 1)
						// Invalidate muxer to trigger reconnect
						p.mu.Lock()
						p.muxer = nil
						p.mu.Unlock()
						return ErrMuxerClosed
					}
					atomic.AddInt32(&successfulEncodes, 1)
					return nil
				},
			},
		}
	}

	p.dialFn = func() (*websocket.Conn, error) {
		// Simulate occasional dial failures
		if atomic.LoadInt32(&muxerCreationCount)%7 == 0 {
			return nil, assert.AnError
		}
		return nil, nil
	}
	p.newClientMuxer = func(ctx context.Context, session MuxerSession, conn *websocket.Conn) providerMuxer {
		return createFlakeyMuxer()
	}

	// Act: Sustained high load for 5 seconds
	const duration = 5 * time.Second
	const numWorkers = 20

	ctx, cancel := context.WithTimeout(context.Background(), duration+5*time.Second)
	defer cancel()

	stopTime := time.Now().Add(duration)
	var wg sync.WaitGroup

	var totalAttempts, totalSuccesses int32

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(stopTime) {
				atomic.AddInt32(&totalAttempts, 1)
				_, err := p.CallStream(ctx, uuid.New(), &api.GetStatus{})
				if err == nil {
					atomic.AddInt32(&totalSuccesses, 1)
				}
				time.Sleep(50 * time.Millisecond) // pace requests
			}
		}()
	}

	wg.Wait()

	// Assert: Most operations should succeed despite flakiness
	attempts := atomic.LoadInt32(&totalAttempts)
	successes := atomic.LoadInt32(&totalSuccesses)
	successRate := float64(successes) / float64(attempts) * 100

	t.Logf("Total attempts: %d, Successes: %d, Success rate: %.2f%%", attempts, successes, successRate)
	t.Logf("Muxers created: %d, Successful encodes: %d, Failed encodes: %d",
		atomic.LoadInt32(&muxerCreationCount),
		atomic.LoadInt32(&successfulEncodes),
		atomic.LoadInt32(&failedEncodes))

	assert.Greater(t, successes, int32(0), "expected at least some successes")
	assert.GreaterOrEqual(t, successRate, 80.0, "expected at least 80%% success rate with retry logic")
}

// TestMuxerGracePeriodPreventsRaceCondition verifies that the grace period
// in replaceMuxer prevents immediate shutdown of in-flight operations.
func TestMuxerGracePeriodPreventsRaceCondition(t *testing.T) {
	// Arrange
	p := NewBidiStreamProvider("https://example.com/", func() (string, error) { return "tok", nil }).(*WebSocketBidiStreamProvider)
	defer p.Close()

	var (
		slowEncodeCalled bool
		slowEncodeErr    error
		mu               sync.Mutex
	)

	// Muxer with an encode that takes 200ms (within grace period)
	slowMuxer := &fakeMuxer{
		pingFn: func() bool { return true },
		bidi: &testBidi{
			encodeFn: func(m any) error {
				mu.Lock()
				slowEncodeCalled = true
				mu.Unlock()
				time.Sleep(200 * time.Millisecond)
				return nil
			},
		},
	}

	fastMuxer := &fakeMuxer{
		pingFn: func() bool { return true },
		bidi: &testBidi{
			encodeFn: func(m any) error {
				return nil
			},
		},
	}

	p.mu.Lock()
	p.muxer = slowMuxer
	p.mu.Unlock()

	// Act: Start encode on slow muxer, then replace it
	ctx := context.Background()
	go func() {
		time.Sleep(50 * time.Millisecond)
		p.replaceMuxer(fastMuxer)
	}()

	// This should use slowMuxer before replacement
	_, err := p.CallStream(ctx, uuid.New(), &api.GetStatus{})
	mu.Lock()
	slowEncodeErr = err
	mu.Unlock()

	// Assert: Slow encode should succeed despite muxer replacement
	// The 500ms grace period should protect it
	assert.True(t, slowEncodeCalled, "slow encode should have been called")
	assert.NoError(t, slowEncodeErr, "slow encode should succeed within grace period")
}

// TestReconnectLoopCoordinatesWithGetOrCreateMuxer verifies that the
// reconnect loop properly uses the dialing flag to avoid dial storms.
func TestReconnectLoopCoordinatesWithGetOrCreateMuxer(t *testing.T) {
	// Arrange
	p := NewBidiStreamProvider("https://example.com/", func() (string, error) { return "tok", nil }).(*WebSocketBidiStreamProvider)
	defer p.Close()

	var dialCount int32

	p.dialFn = func() (*websocket.Conn, error) {
		count := atomic.AddInt32(&dialCount, 1)
		// First dial succeeds, subsequent ones are slow to simulate network issues
		if count == 1 {
			return nil, nil
		}
		time.Sleep(100 * time.Millisecond)
		return nil, nil
	}

	p.newClientMuxer = func(ctx context.Context, session MuxerSession, conn *websocket.Conn) providerMuxer {
		return &fakeMuxer{
			pingFn: func() bool { return false }, // Always unhealthy to trigger reconnect
			bidi: &testBidi{
				encodeFn: func(m any) error {
					return nil
				},
			},
		}
	}

	// Act: Create initial muxer (triggers reconnect loop)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := p.getOrCreateMuxer(ctx)
	require.NoError(t, err)

	// Wait for reconnect loop to detect unhealthy and attempt reconnect
	time.Sleep(1500 * time.Millisecond)

	// Assert: Dial count should be reasonable (not a storm)
	// With coordination, we expect initial dial + a few reconnect attempts
	dials := atomic.LoadInt32(&dialCount)
	t.Logf("Total dials: %d", dials)
	assert.LessOrEqual(t, dials, int32(5), "should not have dial storm - got %d dials", dials)
}
