# Production

Streamkit is **production-ready** and deployed as an embedded library in live services. This guide summarizes what the platform guarantees, what your application must own, and how to operate it reliably.

## Production stack

A typical deployment:

| Layer | Component | Notes |
|-------|-----------|--------|
| Clients | `pkg/client` + `wskit` | One WebSocket per client process; multiplexed logical streams |
| Edge | TLS termination, JWT issuance | Short-lived tokens; scope per store when possible |
| API hosts | `mux` + `wskit.ConfigureWebSocketServer` | Horizontally scaled stateless nodes |
| Coordination | `bus.MessageBus` implementation | Required for cross-instance segment status subscriptions |
| Storage | `azurekit` (multi-node) or `pebblekit` (single-node) | One `Store` instance per store UUID |

See [Operations](operations.md) for deployment topology, health checks, and telemetry.

## Platform guarantees

Streamkit provides:

- **Per-segment total order** — Sequence numbers are monotonic within a segment; concurrent writers are serialized with explicit conflict errors.
- **Durable append** — Committed entries persist in the configured backend (Pebble or Azure Table).
- **At-least-once delivery** on consume paths — Consumers may see duplicates; the platform does not claim exactly-once across reconnects.
- **Transient error classification** — `StreamsError` with `ErrCodeTransient` supports client retry policies.
- **Subscription reconnect** — Retryable failures re-establish the stream; delivery resumes as **latest snapshot → live updates**.
- **Auth-scoped stores** — WebSocket sessions enforce JWT scopes (`streamkit::*` or `streamkit::{storeID}`).

## Application responsibilities

Production services **must** implement:

1. **Durable consumer cursors** — Persist `MinSequence` or lexicographic offsets; recreate consume streams after WebSocket disconnect (consume is not auto-resumed).
2. **Idempotent handlers** — Dedupe on `Entry.Sequence` (and `TRX.ID` when needed) for exactly-once business semantics.
3. **Subscription lifecycle** — Resubscribe after permanent subscription errors; use **consume** for history, subscriptions for head/status only.
4. **JWT refresh** — On dial failure or 401, return a new token from `fetchJWT`; use `OnDialFailure` to invalidate caches.
5. **Observability** — OpenTelemetry exporters, structured logs with `store_id` / `space` / `segment`, alerts on reconnect rate and sequence conflicts.

Details: [Operational contracts](limitations.md).

## Recommended configuration

### Client

```go
provider := wskit.NewBidiStreamProvider(addr, fetchJWT)
// Optional: invalidate token cache on dial failure
if p, ok := provider.(*wskit.WebSocketBidiStreamProvider); ok {
    p.OnDialFailure = func(err error) { tokenCache.Invalidate() }
}

c := client.NewClientWithRetryPolicy(provider, client.AggressiveRetryPolicy())
defer c.Close()
```

- Use `AggressiveRetryPolicy()` on unreliable networks (`NewClientWithRetryPolicy` — handler timeout remains 30s).
- If handlers can exceed 30s, use `NewClientWithHandlerTimeout` (default retry policy) or `NewClientWithMetrics` with `NewOTelClientMetrics()`.
- Always call `Close()` on shutdown.

### Server

```go
nodeManager := server.NewNodeManager(
    server.WithStoreFactory(factory),
    server.WithMessageBusFactory(busFactory),
    server.WithIdleEviction(15*time.Minute, time.Minute),
)
```

- Enable **idle eviction** on multi-tenant hosts to cap open store nodes.
- Wire a production **message bus** (Redis, NATS, or in-cluster pub/sub) when using subscriptions across replicas.
- Expose `Healthz` for load balancers.

### Storage

| Scenario | Backend |
|----------|---------|
| Single API instance, local disk | `pebblekit` with replicated/backed-up volume |
| Horizontally scaled API tier | `azurekit` (shared tables) |
| Disaster recovery | Azure geo-redundant storage + backup policy; Pebble volume snapshots |

## Capacity planning

| Signal | Guidance |
|--------|----------|
| Active segments per client process | Stable below ~10k; monitor client memory if segment keys churn into millions |
| Produces per segment | Per-segment locks serialize writers; scale throughput across segments |
| WebSocket fan-in | One physical connection per client; multiplex many logical streams |
| Azure tables | Watch RU/throttling; batch limits (90 ops, 3.75 MB) are enforced in `azurekit` |

## Failure modes and response

| Event | Expected behavior | Action |
|-------|-------------------|--------|
| WebSocket disconnect | Consume stops; subscriptions enter reconnect loop | Resume consume from stored cursor; monitor `GetSubscriptionStatus` |
| `ErrSequenceMismatch` | Permanent — conflicting producer | Retry produce with correct sequence from `Peek` |
| `ErrSeqNumberBehind` (transient) | Retryable skew | Client retry policy |
| Subscription permanent error | Removed from client registry | Log, alert, explicit `SubscribeToSegment` |
| Node manager circuit open | Store creation blocked 30s after 3 failures | Fix storage connectivity; backoff clients |
| Handler timeout | Subscription callback canceled | Speed up handler or increase timeout |

## Release and versioning

- **Semantic versioning** — API message discriminators are versioned (`streamkit://api/v1/...`); breaking wire changes will introduce new discriminator versions.
- **Go module** — `go get github.com/fgrzl/streamkit@vX.Y.Z` after tagged releases.
- **Changelog** — Review [CHANGELOG](../CHANGELOG.md) before upgrading.

## Pre-launch checklist

- [ ] TLS (`wss://`) on client connections
- [ ] JWT scopes least-privilege per store
- [ ] Message bus shared across API replicas (if using subscriptions)
- [ ] Consumer offset persistence tested through forced disconnect
- [ ] Dedupe logic tested under duplicate delivery
- [ ] Backups and restore drill for storage backend
- [ ] OTel traces/metrics and alerts on error rates
- [ ] Run `go test -race ./...` on your integration branch before deploy

## Related documentation

- [Operational contracts](limitations.md) — boundaries and non-goals
- [Client guide](client-guide.md) — API reference for SDK usage
- [Transport](transport.md) — WebSocket auth and reconnect
- [Storage](storage.md) — backend selection and semantics
