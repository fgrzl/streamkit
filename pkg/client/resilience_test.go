package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldReturnSensibleDefaultsWhenDefaultRetryPolicyCalled(t *testing.T) {
	// Arrange
	// (none)

	// Act
	p := DefaultRetryPolicy()

	// Assert
	assert.GreaterOrEqual(t, p.MaxAttempts, 1)
	assert.Greater(t, p.InitialBackoff, time.Duration(0))
	assert.GreaterOrEqual(t, p.MaxBackoff, p.InitialBackoff)
	assert.Greater(t, p.BackoffMultiplier, 0.0)
}

func TestShouldReturnAggressivePolicyValuesWhenAggressiveRetryPolicyCalled(t *testing.T) {
	// Arrange
	// (none)

	// Act
	p := AggressiveRetryPolicy()

	// Assert
	assert.Equal(t, 8, p.MaxAttempts)
	assert.Equal(t, 200*time.Millisecond, p.InitialBackoff)
	assert.Equal(t, 15*time.Second, p.MaxBackoff)
	assert.Equal(t, 1.5, p.BackoffMultiplier)
}

func TestShouldReturnFalseWhenIsRetryableCalledWithNilError(t *testing.T) {
	// Arrange
	// (none)

	// Act
	ok := IsRetryable(nil)

	// Assert
	assert.False(t, ok)
}

func TestShouldReturnFalseWhenErrorMessageContainsPermanentKeyword(t *testing.T) {
	// Arrange
	// Must match strings in resilience.go permanentErrors list (substring match).
	permanent := []string{
		"unauthorized", "forbidden", "authentication failed",
		"permission denied", "not found", "invalid argument",
		"bad request", "invalid request",
	}

	for _, msg := range permanent {
		err := errors.New(msg)

		// Act
		ok := IsRetryable(err)

		// Assert
		assert.False(t, ok, "expected %q to be non-retryable", msg)
	}
}

func TestShouldReturnTrueWhenErrorMessageContainsTransientKeyword(t *testing.T) {
	// Arrange
	transient := []string{
		"connection refused", "connection reset", "timeout",
		"service unavailable", "503", "504", "429", "throttled",
	}

	for _, msg := range transient {
		err := errors.New(msg)

		// Act
		ok := IsRetryable(err)

		// Assert
		assert.True(t, ok, "expected %q to be retryable", msg)
	}
}

func TestShouldSucceedOnFirstCallWhenFnReturnsNil(t *testing.T) {
	// Arrange
	ctx := context.Background()
	policy := RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: time.Second, BackoffMultiplier: 2}
	calls := 0

	// Act
	err := RetryWithBackoff(ctx, policy, func(context.Context) error {
		calls++
		return nil
	})

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
}

func TestShouldStopImmediatelyWhenFnReturnsNonRetryableError(t *testing.T) {
	// Arrange
	ctx := context.Background()
	policy := RetryPolicy{MaxAttempts: 5, InitialBackoff: time.Millisecond, MaxBackoff: time.Second, BackoffMultiplier: 2}
	calls := 0
	permanent := errors.New("unauthorized")

	// Act
	err := RetryWithBackoff(ctx, policy, func(context.Context) error {
		calls++
		return permanent
	})

	// Assert
	assert.ErrorIs(t, err, permanent)
	assert.Equal(t, 1, calls)
}

func TestShouldReturnContextErrorWhenContextCanceledDuringRetry(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(context.Background())
	policy := RetryPolicy{MaxAttempts: 10, InitialBackoff: 50 * time.Millisecond, MaxBackoff: time.Second, BackoffMultiplier: 2}
	calls := 0

	// Act
	err := RetryWithBackoff(ctx, policy, func(c context.Context) error {
		calls++
		if calls == 2 {
			cancel()
		}
		return errors.New("connection refused")
	})

	// Assert
	assert.ErrorIs(t, err, context.Canceled)
	assert.GreaterOrEqual(t, calls, 2)
}

func TestShouldReturnLastErrorAfterMaxAttemptsWhenAllCallsFailWithRetryableError(t *testing.T) {
	// Arrange
	ctx := context.Background()
	policy := RetryPolicy{MaxAttempts: 3, InitialBackoff: 1 * time.Millisecond, MaxBackoff: 10 * time.Millisecond, BackoffMultiplier: 2}
	calls := 0
	transient := errors.New("connection refused")

	// Act
	err := RetryWithBackoff(ctx, policy, func(context.Context) error {
		calls++
		return transient
	})

	// Assert
	assert.ErrorIs(t, err, transient)
	assert.Equal(t, 3, calls)
}

func TestShouldReturnFalseWhenErrorIsContextCanceledOrDeadlineExceeded(t *testing.T) {
	// Arrange
	// (context errors are never retryable)

	// Act
	canceledOK := IsRetryable(context.Canceled)
	deadlineOK := IsRetryable(context.DeadlineExceeded)

	// Assert
	assert.False(t, canceledOK)
	assert.False(t, deadlineOK)
}

func TestShouldReturnContextErrorImmediatelyWhenContextAlreadyCanceled(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	policy := RetryPolicy{MaxAttempts: 5, InitialBackoff: time.Hour, MaxBackoff: time.Hour, BackoffMultiplier: 2}
	calls := 0

	// Act
	err := RetryWithBackoff(ctx, policy, func(context.Context) error {
		calls++
		return errors.New("connection refused")
	})

	// Assert
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 0, calls)
}

func TestShouldRunAtLeastOnceWhenPolicyMaxAttemptsIsZero(t *testing.T) {
	// Arrange
	ctx := context.Background()
	policy := RetryPolicy{MaxAttempts: 0, InitialBackoff: time.Millisecond, MaxBackoff: time.Second, BackoffMultiplier: 2}
	calls := 0

	// Act
	err := RetryWithBackoff(ctx, policy, func(context.Context) error {
		calls++
		return nil
	})

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
}
