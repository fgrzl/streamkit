# Streamkit Resilience Guide

## Overview

Resilience has been integrated directly into the Streamkit client. The client now automatically implements retry logic with exponential backoff at both the transport and application layers for seamless recovery from transient failures.

## Changes Made

### 1. Enhanced Transport Layer (`pkg/transport/wskit/provider.go`)

#### Improved CallStream Retry Logic
- **Increased retry attempts**: From 3 to 7 attempts for muxer closure recovery
- **Exponential backoff**: Now uses true exponential backoff (`baseDelay * 2^(attempt-1)`) instead of linear backoff
- **Backoff range**: 100ms → 10s with jitter to prevent thundering herd
- **Better logging**: Enhanced diagnostics with attempt tracking and backoff duration visibility

```go
// Retry behavior:
// Attempt 1: 100ms ± jitter
// Attempt 2: 200ms ± jitter  
// Attempt 3: 400ms ± jitter
// Attempt 4: 800ms ± jitter
// Attempt 5-7: capped at 10s ± jitter
```

#### Coordinated Reconnection in `getOrCreateMuxer`
- **Waiting for in-progress dials**: Instead of immediately failing when another goroutine is dialing, now waits up to 15 seconds for completion
- **Concurrent resilience**: Multiple goroutines can coordinate recovery by waiting for the first successful dial attempt
- **Prevents dial storms**: Only one goroutine at a time performs the actual dial

### 2. Application Layer Resilience (`pkg/client/`)

The Client implementation now includes built-in resilience for transient failures.

#### RetryPolicy Configuration (`resilience.go`)
```go
type RetryPolicy struct {
    MaxAttempts       int           // Number of retry attempts
    InitialBackoff    time.Duration // Starting backoff delay
    MaxBackoff        time.Duration // Maximum backoff duration
    BackoffMultiplier float64       // Exponential growth factor
}
```

#### Built-in Policies
- **DefaultRetryPolicy()**: Conservative policy for normal conditions
  - 5 attempts, 100ms initial, 5s max, 2.0x multiplier

- **AggressiveRetryPolicy()**: For production environments with unstable networks
  - 8 attempts, 200ms initial, 15s max, 1.5x multiplier

#### Direct Client Integration (`client.go`)
All client operations now automatically use retry policy:

```go
// The client includes built-in resilience - no wrapper needed
provider := wskit.NewBidiStreamProvider(addr, tokenFn)
client := client.NewClient(provider)  // Uses DefaultRetryPolicy()

// Or with custom policy
client := client.NewClientWithRetryPolicy(provider, client.AggressiveRetryPolicy())

// Use normally - resilience is transparent
entry, err := client.Peek(ctx, storeID, space, segment)
if err != nil {
    // Already retried with exponential backoff
    return err
}
```

## Behavior Under Failure

### Scenario: Muxer Closes During Request

1. **Provider layer** (wskit/provider.go):
   - Detects `ErrMuxerClosed`
   - Triggers background reconnection attempt via `getOrCreateMuxer`
   - Waits for reconnect with exponential backoff (100ms, 200ms, 400ms, etc.)
   - Retries up to 7 times before failing

2. **Application layer** (client methods):
   - If provider fails, catches the error
   - Applies application-level retry policy
   - Waits with exponential backoff (configurable)
   - Retries stream setup for operations like Peek, Consume, etc.

3. **Background recovery** (provider reconnect loop):
   - Continuous health checks every 1 second
   - If muxer becomes unhealthy, establishes new connection
   - Exponential backoff on dial failures (1s, 2s, 4s, ..., 30s max)
   - Automatically resets on successful connection

## Logging and Observability

Enhanced logging provides visibility into retry behavior:

```
WARN provider: muxer closed during encode, retrying attempt=1 maxAttempts=7 channelID=...
DEBUG provider: encode retry backoff backoff=100ms attempt=1
INFO provider: initial muxer created
INFO provider: reconnect successful
```

Application-level retries also log:
```
WARN operation failed, retrying attempt=1 maxAttempts=5 backoff=100ms error=...
ERROR exhausted retry attempts maxAttempts=5 lastError=...
```

## Migration Guide

### For Existing Code

No changes required! Resilience is now built in:

```go
// This automatically has resilience
provider := wskit.NewBidiStreamProvider(addr, tokenFn)
client := client.NewClient(provider)
entry, _ := client.Peek(ctx, storeID, space, segment)
```

### For Production Deployments

Use custom retry policy if needed:

```go
provider := wskit.NewBidiStreamProvider(addr, tokenFn)
client := client.NewClientWithRetryPolicy(
    provider,
    client.AggressiveRetryPolicy(),
)

// Use client throughout your application
```

### Custom Retry Policy

```go
customPolicy := client.RetryPolicy{
    MaxAttempts:       10,
    InitialBackoff:    500 * time.Millisecond,
    MaxBackoff:        30 * time.Second,
    BackoffMultiplier: 1.8,
}

client := client.NewClientWithRetryPolicy(provider, customPolicy)
```

## Performance Considerations

1. **Total retry timeout**: For DefaultRetryPolicy, maximum wait time is ~16 seconds
   - 100ms + 200ms + 400ms + 800ms + 1.6s + 3.2s + 6.4s + 3.2s

2. **Context cancellation**: All retries respect `context.Context` deadlines
   - If context is cancelled, retries stop immediately

3. **Background reconnection**: Runs independently and continuously
   - Helps recover muxer health proactively
   - Reduces retry backoff duration in best-case scenarios

## Testing

All existing tests pass with the improvements:
```bash
go test ./... -v -race
```

Key test coverage:
- `TestGetOrCreateMuxerRetries`: Validates provider retry logic
- `TestCallStreamRetriesOnMuxerClosed`: Verifies muxer closure recovery
- `TestBackgroundReconnectRecreatesMuxer`: Confirms background reconnection
- Full integration tests with Azure, Pebble, and in-process backends

## Troubleshooting

### Still seeing "muxer closed" errors?

1. Check network connectivity to the server
2. Review server logs for connection issues
3. Consider using `AggressiveRetryPolicy` instead of default
4. Enable DEBUG logging to see retry attempts:
   ```go
   // Via environment variable or slog.SetDefault()
   slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
       Level: slog.LevelDebug,
   })))
   ```

### High latency due to retries?

1. Context timeout is too short - increase it
2. Use `DefaultRetryPolicy` instead of `AggressiveRetryPolicy`
3. Monitor network to server - high packet loss increases retry frequency
4. Reduce `MaxAttempts` with a custom policy if needed

## Future Enhancements

Potential improvements for consideration:
1. Circuit breaker pattern for rapid failure detection
2. Adaptive backoff based on observed recovery times
3. Metrics/tracing integration for retry observability
4. Per-operation retry policies for fine-grained control
5. Jitter strategy customization (exponential, full, equal)
