# Server guide

Server-side logic lives in `github.com/fgrzl/streamkit/pkg/server`. Applications embed a **node manager** and expose it through a transport (typically `wskit`).

## Node

A **node** handles one store’s traffic:

```go
type Node interface {
    Handle(context.Context, api.BidiStream)
    Close()
}
```

Applications normally obtain nodes via `NodeManager.GetOrCreate` — not by calling `NewNode` directly. Each node `Handle`s streams as follows:

1. Decodes the first `polymorphic.Envelope` from the stream.
2. Dispatches by message type (`Produce`, `ConsumeSegment`, `SubscribeToSegmentStatus`, lease ops, etc.).
3. Encodes responses on the same `BidiStream` until the operation completes or the stream ends.

Handler panics are recovered and logged; the stream is closed with error.

## Node manager

```go
nodeManager := server.NewNodeManager(
    server.WithStoreFactory(factory),
    server.WithMessageBusFactory(busFactory),
    server.WithIdleEviction(10*time.Minute, time.Minute), // optional
)
defer nodeManager.Close()
```

| Method | Behavior |
|--------|----------|
| `GetOrCreate(ctx, storeID)` | Returns a node, creating storage if needed |
| `Remove(ctx, storeID)` | Closes and evicts a store node |
| `Close()` | Shuts down all nodes |

**Circuit breaker:** After three consecutive store creation failures for a store ID, further `GetOrCreate` calls fail fast for 30 seconds.

**Idle eviction:** When configured, nodes with no active `Handle` calls and no recent access are closed and removed.

## Message bus and notifications

Segment status changes are published to the bus (`pkg/bus`) using routes derived from `api.GetSegmentNotificationRoute`. Subscribers on the server side forward notifications to active `SubscribeToSegmentStatus` streams.

Your deployment must supply a `bus.MessageBusFactory` if you use subscriptions. Integration tests use an in-memory implementation (`test/harness_test.go`).

## Subscription routing

`subscription_router.go` groups subscribers per space, applies heartbeat validation when configured, and coordinates snapshot delivery on subscribe and after reconnect.

Heartbeat interval from clients is clamped between 1 and 300 seconds when non-zero.

## Leases

Lease acquire, renew, and release messages are handled against `internal/lease.Store` attached to the node. `client.WithLease` is the supported client API.

Maximum lease TTL on the server is 24 hours.

## Consume limits

The server clamps `Limit` on consume requests to `serverMaxConsumeEntries` (10,000,000) to bound work per request.

## Channel context

`server.WithChannelID(ctx, id)` / `ChannelIDFromContext(ctx)` attach a logical channel UUID for tracing. Transports set this when registering streams.

## Wiring with WebSocket

```go
router := mux.NewRouter()
mux.UseAuthentication(router, mux.WithAuthValidator(validator.Validate))
mux.UseAuthorization(router)
wskit.ConfigureWebSocketServer(router, nodeManager)
```

Clients connect to `GET /streamz` (WebSocket upgrade). Authorization uses JWT scopes:

- `streamkit::*` — all stores
- `streamkit::{storeUUID}` — single store

See [Transport](transport.md) for session details.

## Operational notes

- One node per store ID in memory; large multi-tenant hosts should size memory for active stores or enable idle eviction.
- Store creation errors should be monitored — repeated failures trip the circuit breaker for that store.
- For notification-heavy workloads, ensure the message bus implementation can deliver to all subscribed server handlers without blocking produce paths.
