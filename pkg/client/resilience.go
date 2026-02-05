// Package client provides resilience utilities for error recovery and retry strategies.
package client

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// RetryPolicy defines backoff and attempt configuration for retryable operations.
type RetryPolicy struct {
	// MaxAttempts is the maximum number of attempts (including initial attempt).
	// Default: 3
	MaxAttempts int
	// InitialBackoff is the initial backoff duration for exponential backoff.
	// Default: 100ms
	InitialBackoff time.Duration
	// MaxBackoff is the maximum backoff duration between retries.
	// Default: 10s
	MaxBackoff time.Duration
	// BackoffMultiplier is the exponential backoff multiplier.
	// Default: 2.0
	BackoffMultiplier float64
}

// DefaultRetryPolicy returns a sensible retry policy for streaming operations.
// Supports short-lived transient failures in muxer reconnection.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:       5,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        5 * time.Second,
		BackoffMultiplier: 2.0,
	}
}

// AggressiveRetryPolicy returns a policy for operations that can tolerate longer retry windows.
// Useful for production scenarios with less stable networks.
func AggressiveRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:       8,
		InitialBackoff:    200 * time.Millisecond,
		MaxBackoff:        15 * time.Second,
		BackoffMultiplier: 1.5,
	}
}

// IsRetryable determines if an error should trigger a retry.
// Returns true for transient network errors, false for permanent failures.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	// Context cancellation is not retryable
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// For now, assume most errors are transient (network hiccups, muxer closures, etc.)
	// In production, you may want to check for specific error types that are permanent
	// (e.g., auth failures, not found, etc.)
	return true
}

// RetryWithBackoff executes fn with exponential backoff retry on failure.
// Returns immediately on success or if ctx is cancelled.
func RetryWithBackoff(ctx context.Context, policy RetryPolicy, fn func(context.Context) error) error {
	if policy.MaxAttempts < 1 {
		policy.MaxAttempts = 1
	}
	if policy.InitialBackoff < 1 {
		policy.InitialBackoff = 100 * time.Millisecond
	}
	if policy.MaxBackoff < policy.InitialBackoff {
		policy.MaxBackoff = 10 * time.Second
	}
	if policy.BackoffMultiplier <= 0 {
		policy.BackoffMultiplier = 2.0
	}

	var lastErr error
	backoff := policy.InitialBackoff

	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := fn(ctx)
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if error is retryable
		if !IsRetryable(err) {
			slog.WarnContext(ctx, "non-retryable error, aborting retries", slog.String("error", err.Error()))
			return err
		}

		if attempt == policy.MaxAttempts {
			slog.ErrorContext(ctx, "exhausted retry attempts",
				slog.Int("maxAttempts", policy.MaxAttempts),
				slog.String("lastError", err.Error()))
			return err
		}

		slog.WarnContext(ctx, "operation failed, retrying",
			slog.Int("attempt", attempt),
			slog.Int("maxAttempts", policy.MaxAttempts),
			slog.Duration("backoff", backoff),
			slog.String("error", err.Error()))

		// Wait with backoff
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		// Exponential backoff for next attempt
		nextBackoff := time.Duration(float64(backoff) * policy.BackoffMultiplier)
		if nextBackoff > policy.MaxBackoff {
			nextBackoff = policy.MaxBackoff
		}
		backoff = nextBackoff
	}

	return lastErr
}
