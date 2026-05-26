# Operational contracts

This document defines **stable behavioral contracts** for production use. These are not temporary gaps—they describe how Streamkit interacts with your application so you can design correct consumers and producers.

For deployment patterns and checklists, see [Production](production.md).

## Delivery semantics

### At-least-once consumption

Committed entries are durable, but consume streams can deliver the same entry more than once when:

- A logical stream is retried after a transient error
- Offsets are resumed with edge timing around timestamps
- Concurrent producers write across segments in a space-scoped consume

**Contract:** Your handlers must be idempotent or dedupe using `Entry.Sequence` (and transaction metadata when required).

### Consume streams and disconnect

When the WebSocket transport drops, **active consume enumerators do not resume automatically**.

**Contract:** Persist cursors in application storage (`MinSequence`, `ConsumeSpace.Offset`, or `Consume.Offsets`) and open a new consume after reconnect. Resilience offset updates apply only within a single retry sequence, not across process restarts.

## Subscriptions

Subscriptions deliver **segment status** (head sequence, timestamps)—not a durable event log.

| Behavior | Contract |
|----------|----------|
| Reconnect (retryable) | Latest snapshot for matching segments, then live updates |
| Permanent error | Subscription removed; application must resubscribe |
| History / replay | Use `ConsumeSegment` or `ConsumeSpace`, not subscriptions |
| Missed updates while offline | Not replayed beyond snapshot at reconnect |

**Contract:** Treat subscriptions as **live inventory signals**. Persist business state from consume, not from subscription callbacks alone.

## Worker presence

Worker presence is **authoritative on a single server node** per store (in-memory map). **Many clients** connect to that node: some register as workers (`WithWorkerID`), others observe inventory only. The server fans out the full inventory to every connected observer on connect and on each change.

| Behavior | Contract |
|----------|----------|
| Deployment | One streamkit server node per store; not replicated across API instances |
| Clients | Many subscribers and many workers per store on the same node |
| Subscribe stream open | Initial inventory snapshot, then updates on changes |
| Worker stream close | Worker removed from inventory; all subscribers receive updated inventory |
| Stale worker | Evicted when client renewals stop (~3× renewal interval, minimum 30s) |
| Duplicate `worker_id` | New registration replaces the prior stream for that ID |
| Subscribe reconnect | Latest inventory snapshot at reconnect |
| Missed updates while offline | Not replayed beyond snapshot at reconnect |
| Work rebalancing | Application responsibility |

**Contract:** Use `SubscribeWorkers` with optional `WithWorkerID(uuid.UUID)` as ephemeral capacity signals. Maintain local state from inventory payloads on each client.

## Produce and sequencing

- Each record carries an explicit `Sequence`; the server rejects non-contiguous appends.
- Concurrent producers to the same segment: one wins; others receive sequence or transaction errors.
- **Contract:** Use `Peek` or `SegmentStatus.LastSequence` to determine the next sequence; handle `ErrSequenceMismatch` as non-retryable.

## Client resource behavior

### Per-segment produce locks

The client maintains per-segment locks for produce ordering. Locks are not evicted automatically.

| Workload | Expectation |
|----------|-------------|
| &lt; 10k stable segments per process | No action required |
| High churn (millions of unique segment keys over process lifetime) | Monitor heap; plan process recycling or future LRU (roadmap) |

## Server and storage

| Topic | Contract |
|-------|----------|
| Consume `Limit` | Clamped server-side; very large limits are bounded per request |
| Capped multi-segment consume | Entry set may vary under concurrent writes when `Limit` &lt; ∞ |
| Azure WAL recovery | Background recovery for pending transactions; define RPO/RTO with your Azure policies |
| API versioning | Wire format uses `v1` discriminators; new major versions will use parallel discriminators |

## Error classification

Use `errors.As` into `*api.StreamsError`:

| Code | Retry |
|------|-------|
| `ErrCodeTransient` | Yes, with `RetryPolicy` / `IsRetryable` |
| `ErrCodePermanent` | No — fix request or cursor |

Context cancellation and deadline errors are never retryable.

## Non-goals

Streamkit does **not** provide:

- Kafka-style consumer groups with broker-managed offsets
- Exactly-once end-to-end delivery without application deduplication
- Durable subscription cursors or missed-update replay
- A hosted managed service (you operate storage and API processes)

## Roadmap

Enhancements under consideration (not required for current production deployments):

- LRU eviction for per-segment client locks
- `OnSubscriptionFailed` callback
- Prometheus metrics in the client
- Batch produce API for very high throughput

Track progress in [CHANGELOG](../CHANGELOG.md).
