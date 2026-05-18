# Transport

Transports implement `api.BidiStream` and `api.BidiStreamProvider`, moving polymorphic envelopes between client and server.

## Comparison

| | `inprockit` | `wskit` |
|---|-------------|---------|
| **Medium** | Go channels | WebSocket |
| **Serialization** | Often direct; JSON fallback | JSON over frames |
| **Multiplexing** | `InProcMuxer` | `WebSocketMuxer` |
| **Auth** | None | JWT via `fgrzl/mux` |
| **Reconnect** | N/A (never disconnects) | Jittered backoff, listener API |
| **Typical use** | Unit tests, benchmarks | Production |
| **Channel buffer** | 64 messages per direction | Multiplexed frames on one connection |

## In-process (`inprockit`)

```go
provider := inprockit.NewInProcBidiStreamProvider(ctx, nodeManager)
client := client.NewClient(provider)
```

`CallStream` registers a client/server stream pair on the muxer, encodes the initial envelope, and returns the client side. The server node handles the peer stream in another goroutine.

`RegisterReconnectListener` is a no-op.

## WebSocket (`wskit`)

### Client provider

```go
provider := wskit.NewBidiStreamProvider("wss://api.example.com", func() (string, error) {
    return tokenFromYourAuthService()
})
c := client.NewClient(provider)
```

The provider normalizes the address to end with `/streamz`. One physical WebSocket connection is shared; logical streams are multiplexed with `MuxerMsg` frames (control type, store ID, channel ID, payload).

**Dial behavior:**

- JWT fetched on every dial attempt
- Exponential backoff with jitter on failures
- `OnDialFailure` — hook to invalidate cached tokens
- `RetryAuthFailures` — optional retry on 401 during key rotation

### Server endpoint

```go
wskit.ConfigureWebSocketServer(router, nodeManager, opts...)
// mounts GET /streamz
```

Requires authenticated `mux.RouteContext` user. `NewServerMuxerSession` enforces store access from JWT scopes (`streamkit::*` or `streamkit::{storeID}`).

### Connection lifecycle

- Heartbeat ping/pong on the physical connection
- Write pump with queued frames
- Logical stream overload may return permanent errors (not retried)
- Tombstone eviction for stale logical streams under pressure (see transport tests)

### Reconnect listeners

```go
provider.RegisterReconnectListener(api.ReconnectListenerFunc(func() {
    // informational — subscription replay uses client dispatcher
}))
```

Subscriptions: retryable failures stay in the reconnect loop; on success, **latest snapshot → live updates**.

## Security checklist

- Terminate TLS at the load balancer or server (`wss://`).
- Issue short-lived JWTs; refresh on `OnDialFailure` / 401.
- Scope tokens per store when possible.
- Do not log JWTs or payloads at Info level.

See [Operations](operations.md) for local WebSocket testing with `httptest` and JWT (pattern in `test/harness_test.go`).
