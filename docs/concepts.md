# Core concepts

Understanding these terms is essential for using Streamkit correctly.

## Hierarchy

```
Store (UUID)
 └── Space (string)
      └── Segment (string)
           └── Entry (ordered by Sequence)
```

### Store

A **store** is identified by a `uuid.UUID`. It is the unit of physical isolation: each store maps to its own storage instance (Pebble directory or Azure table prefix). All spaces and segments live inside one store.

Use separate store IDs for tenants, environments, or datasets that must not share storage.

### Space

A **space** is a logical container for related segments — for example `orders`, `inventory`, or an event-sourcing **area** name. Spaces are created implicitly when you first produce to a segment in that space.

**ConsumeSpace** reads across all segments in a space, merging entries by **timestamp** (not by segment name). Use this when you need a unified timeline; use **ConsumeSegment** when you need strict sequence order within one segment.

### Segment

A **segment** is an independent, strictly ordered sub-stream within a space. Sequence numbers start at 1 and increase monotonically per segment. Concurrent producers to the same segment are coordinated; conflicting writes surface as sequence errors.

Segments are the right granularity for:

- One aggregate in event sourcing (segment = aggregate ID)
- One device or partition key
- Any stream that must preserve total order

## Data model

### Record (write)

A **record** is what you send on produce:

| Field | Meaning |
|-------|---------|
| `Sequence` | Expected sequence in the segment (must match server state for append) |
| `Payload` | Opaque bytes (often JSON or encoded envelopes) |
| `Metadata` | Optional string map |

### Entry (read)

An **entry** is what you receive on consume or peek:

| Field | Meaning |
|-------|---------|
| `Sequence` | Position in the segment |
| `Timestamp` | Wall-clock ordering key for space-level merges |
| `TRX` | Transaction metadata (`ID`, `Node`, `Number`) |
| `Payload` / `Metadata` | Stored record content |
| `Space` / `Segment` | Location (populated on read paths) |

### SegmentStatus

Metadata about a segment’s head and tail: first/last sequence and timestamps. Delivered after produce and via **subscriptions** when inventory changes.

## Consumption modes

| Operation | Ordering | Scope |
|-----------|----------|--------|
| `ConsumeSegment` | Strict sequence in one segment | Single segment |
| `ConsumeSpace` | By timestamp across segments | All segments in a space |
| `Consume` | By timestamp across multiple spaces | Configurable space/offset map |
| `Peek` | Latest entry only | Single segment; empty segments return an entry with `Space` and `Segment` set and zero sequences |

### Offsets and cursors

Consumers resume by passing:

- **ConsumeSegment** — `MinSequence`, and optionally `MinTimestamp` / `MaxTimestamp`
- **ConsumeSpace** — `Offset` as a lexicographic key (`lexkey.LexKey`), typically from `Entry.GetSpaceOffset()`
- **Consume** — per-space `Offsets` map

After reading an entry, persist the offset or sequence your application needs to resume. Streamkit does not maintain durable consumer groups.

### Limits

`Limit` on consume requests caps how many entries are returned in one stream. Zero or omitted means no cap (subject to server maximum). Under concurrent writes, which entries appear within a finite limit may vary — design consumers to be idempotent.

## Transactions

Each produce batch is associated with **transaction metadata** (`TRX`) on stored entries. The storage layer uses transactions for consistent multi-record writes and recovery (especially on Azure). Clients normally interact via produce; transaction errors (`api.ErrTrxIDMismatch`, `api.ErrSequenceMismatch`, etc.) indicate conflicting concurrent writers or invalid sequence expectations. See [API protocol](api-protocol.md).

## Subscriptions

**SubscribeToSegment** (or **SubscribeToSpace**, which subscribes to all segments in a space via `Segment: "*"`) delivers `SegmentStatus` updates over a long-lived stream.

Contract on reconnect:

1. Latest-state **snapshot** for matching segments
2. Then **live** updates

There is no durable missed-update replay or cursor in the current API. Applications that need full history must consume from storage.

## Leases

**WithLease** acquires a distributed lease for a key within a store. The callback receives a context canceled when the lease is lost, released, or the parent context ends. Use for leader election or single-writer coordination across instances.

## Storage key prefixes

Internal keys use stable prefixes (see `pkg/api/constants.go`):

| Constant | Role |
|----------|------|
| `DAT` | Data entries |
| `INV` | Space/segment inventory |
| `TXN` | Transaction records |
| `SPACES` / `SEGMENTS` | Key path components for ordering |

Entry helpers `GetSpaceOffset()` and `GetSegmentOffset()` build lex keys for cursor storage.
