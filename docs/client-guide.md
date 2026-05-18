# Client guide

The public SDK lives in `github.com/fgrzl/streamkit/pkg/client`. All operations are scoped by **store ID** (`uuid.UUID`).

## Creating a client

```go
import "github.com/fgrzl/streamkit/pkg/client"

c := client.NewClient(provider)
defer c.Close()
```

| Constructor | Behavior |
|-------------|----------|
| `NewClient(provider)` | Default retry policy; 30s subscription handler timeout |
| `NewClientWithHandlerTimeout(provider, timeout)` | Custom handler timeout (`<= 0` disables timeout; handlers run inline) |
| `NewClientWithRetryPolicy(provider, policy)` | Custom retry policy; 30s handler timeout (fixed) |
| `NewClientWithMetrics(provider, timeout, metrics)` | Custom timeout and optional `ClientMetrics` (`NewOTelClientMetrics()`) |
| `NewClientWithTracing(provider)` | Wraps client with OpenTelemetry tracing (default 30s handler timeout) |
| `NewClientWithTracingAndHandlerTimeout(provider, timeout)` | Tracing + custom handler timeout |

There is no single constructor that sets both a custom `RetryPolicy` and a custom handler timeout. Pick the closest constructor or open an issue if you need both tunables exposed together.

The `provider` must implement `api.BidiStreamProvider` (`inprockit` or `wskit`).

## Client interface

| Method | Purpose |
|--------|---------|
| `GetSpaces` | List space names |
| `GetSegments` | List segments in a space |
| `Peek` | Read latest entry without consuming |
| `Consume` | Multi-space, timestamp-ordered read |
| `ConsumeSpace` | All segments in one space, timestamp-ordered |
| `ConsumeSegment` | Strict sequence order in one segment |
| `Produce` | Write a stream of records |
| `Publish` | Write one record |
| `SubscribeToSpace` | Status updates for all segments in a space |
| `SubscribeToSegment` | Status updates for one segment |
| `GetSubscriptionStatus` | Health metrics for a subscription ID |
| `WithLease` | Run work under a distributed lease |
| `Close` | Shut down background goroutines |

Type aliases re-export API types: `Entry`, `Record`, `ConsumeSegment`, etc.

## Produce and sequences

Each record includes an explicit `Sequence`. The server rejects batches that do not match the next expected sequence (or conflict with concurrent writers). After a successful produce, read `SegmentStatus.LastSequence` from the returned enumerator.

For event sourcing, prefer [`eskit`](event-sourcing.md), which validates contiguous sequences before produce.

## Consumption patterns

**Tail a segment** (ordered):

```go
args := &client.ConsumeSegment{
    Space: "orders", Segment: "order-42", MinSequence: lastSeq + 1,
}
entries := c.ConsumeSegment(ctx, storeID, args)
for entries.MoveNext() {
    entry, err := entries.Current()
    if err != nil {
        return err
    }
    // process entry; persist entry.Sequence for resume
}
```

**Fan-in by time** (space):

```go
args := &client.ConsumeSpace{Space: "orders", Offset: lastOffset}
```

Use `entry.GetSpaceOffset()` from the previous entry as the next `Offset`.

## Subscriptions

```go
sub, err := c.SubscribeToSegment(ctx, storeID, "orders", "order-42", func(status *client.SegmentStatus) {
    if status.Heartbeat {
        return
    }
    // react to LastSequence / timestamps
})
// later: sub.Unsubscribe()
```

`SubscribeToSpace` is implemented as segment status subscription with `Segment: "*"` for the space.

**Reconnect behavior:** retryable failures re-enqueue the subscription; on success you receive a **snapshot** of current segment statuses, then live updates. `OnReconnected` on the provider is informational.

**Permanent failures** remove the subscription from the client registry — log and resubscribe explicitly.

Optional application heartbeats: set heartbeat interval on the underlying API message when using lower-level APIs; default client behavior relies on transport closure.

Monitor health:

```go
st := c.GetSubscriptionStatus(sub.ID())
```

Subscription callbacks run on a worker pool (default 50 concurrent handlers per client). Slow handlers block pool slots until they complete or hit the handler timeout.

## Resilience and retries

`pkg/client/resilience.go` defines retry policies:

| Policy | Max attempts | Initial backoff | Max backoff | Multiplier |
|--------|--------------|-----------------|-------------|------------|
| `DefaultRetryPolicy()` | 5 | 100ms | 5s | 2.0 |
| `AggressiveRetryPolicy()` | 8 | 200ms | 15s | 1.5 |

`IsRetryable(err)` returns false for context cancellation, permanent `StreamsError`, and common auth/validation failures.

**Important:** Retries apply to opening streams and transient failures. A **consume stream is not automatically resumed** after WebSocket disconnect — persist offsets and recreate the consumer.

The resilience enumerator updates offsets on retry for `ConsumeSpace` and `Consume`; still implement sequence-based deduplication for critical paths.

## Leases

```go
err := c.WithLease(ctx, storeID, "leader", 30*time.Second, func(ctx context.Context) error {
    // ctx canceled when lease lost or parent canceled
    return runLeaderWork(ctx)
})
```

Best-effort: if renewal fails, the callback context is canceled.

## WebSocket provider hooks

When using `wskit.WebSocketBidiStreamProvider`:

- `fetchJWT` — called on every dial; must return a fresh token after 401
- `OnDialFailure` — invalidate token cache when dial fails
- `RetryAuthFailures` — treat 401 as transient during JWKS rotation (off by default)
- `RegisterReconnectListener` — notified after reconnect (subscriptions replay via dispatcher)

## Closing

Always call `Close()` when shutting down the client to stop subscription and reconnect goroutines. Safe to call multiple times.

See [Operational contracts](limitations.md) and [Production](production.md) for delivery semantics, cursor persistence, and subscription behavior in live environments.
