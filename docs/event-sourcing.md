# Event sourcing

`github.com/fgrzl/streamkit/pkg/eskit` implements `es.Store` from `github.com/fgrzl/es` on top of the Streamkit client.

## Mapping

| `fgrzl/es` concept | Streamkit mapping |
|--------------------|-------------------|
| `entity.TenantID` | Store UUID |
| `entity.Area` | Space name |
| `entity.ID` | Segment name (aggregate ID string) |
| Event sequence | Record / entry `Sequence` |
| Event payload | JSON `polymorphic.Envelope` wrapping `es.DomainEvent` |

## Usage

```go
import (
    "github.com/fgrzl/es"
    "github.com/fgrzl/streamkit/pkg/client"
    "github.com/fgrzl/streamkit/pkg/eskit"
)

streamStore := eskit.NewStreamStore(client)
```

### Load events

`LoadEvents(ctx, entity, minSequence)` calls `ConsumeSegment` on `entity.TenantID` (store UUID) from `minSequence`, with `entity.Area` as space and `entity.ID.String()` as segment. Each payload is unmarshaled into `es.DomainEvent`.

### Save events

`SaveEvents(ctx, entity, events, expectedSequence)`:

1. Validates `events[0].GetSequence() == expectedSequence + 1`
2. Validates contiguous sequences in the batch
3. Produces records via `client.Produce`
4. Requires at least one `SegmentStatus` in the response

## Sequence contract

The adapter enforces strict sequence rules before write:

- Caller passes **last known sequence** as `expectedSequence`
- First event must be `expectedSequence + 1`
- Each subsequent event increments by 1

The underlying platform is **at-least-once**; partial retries may duplicate. Consumers should use sequence continuity for idempotency (as documented on `SaveEvents`).

## Errors

Unmarshal failures and type cast failures are logged with store, space, segment, and sequence fields. Surface these in application metrics.

## When to use eskit

Use eskit when you already model domains with `fgrzl/es` aggregates and want Streamkit as the physical event log. For custom payloads or non-ES shapes, use `client.Produce` / `ConsumeSegment` directly.

See [Client guide](client-guide.md) for produce/consume details and [Operational contracts](limitations.md) for at-least-once delivery and deduplication in production.
