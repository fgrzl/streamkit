# Streamkit — AI Coding Agent Instructions

## Project Overview

**Streamkit** is a high-throughput, hierarchical event streaming platform written in **Go 1.25.6**. It provides scalable, organized, and reliable data flows with three core concepts:

- **Store**: Physical separation at storage level, identified by `uuid.UUID`. Root container for all spaces and segments.
- **Space**: Top-level logical container for related streams within a store.
- **Segment**: Independent, ordered sub-stream within a Space. Strict ordering within a segment; parallel consumption across segments.

Architectural principle: **Separation of concerns** — logical hierarchy (Space/Segment) is independent from physical storage backends.

## Architecture

```
pkg/
├── api/          Domain models, interfaces, message types, error classification
├── client/       Public SDK (Client interface, resilience, retry policies)
├── eskit/        Event sourcing adapter (bridges streamkit with fgrzl/es)
├── server/       Request routing, Node + NodeManager, channel context
├── storage/      Persistence abstraction
│   ├── pebblekit/   PebbleDB local storage
│   └── azurekit/    Azure Table Storage (custom HTTP client, WAL, batch writes)
├── transport/    Protocol implementations
│   ├── inprockit/   In-process (direct channel-based, no serialization overhead)
│   └── wskit/       WebSocket (multiplexed streams, heartbeat, JWT auth, sessions)
internal/
├── codec/       Binary serialization + Zstd compression
├── cache/       TTL-based expiring cache (sync.Map + cleanup goroutine)
└── txn/         Transaction model for consistent multi-entry writes
```

> `grpckit/` and `http2kit/` exist as placeholders (`.gitkeep` only) — not yet implemented.

### API Layer (`pkg/api/`)

Defines all domain models and interfaces:

- **Models**: `Entry`, `Record`, `SegmentStatus`, `TRX` (transaction metadata)
- **Messages** (each implements `GetDiscriminator() string`): `Consume`, `ConsumeSpace`, `ConsumeSegment`, `Peek`, `Produce`, `GetSpaces`, `GetSegments`, `GetStatus`, `SubscribeToSegmentStatus`, `SegmentNotification`
- **Interfaces**: `BidiStream` (bidirectional encode/decode/close), `BidiStreamProvider` (creates streams per store), `Routeable` (polymorphic marker for stream calls), `Consumable` (polymorphic + `GetSpaces()` for routing), `Subscription` (cancellable subscription handle)
- **Errors**: `StreamsError` with `ErrCodeTransient` (retryable) and `ErrCodePermanent` (terminal). Predefined error vars: `ERR_SEQUENCE_MISMATCH`, `ERR_TRX_ID_MISMATCH`, etc.
- **Constants**: `DAT`, `INV`, `TXN`, `SPACES`, `SEGMENTS` — prefixes for all storage keys
- **Registry**: `EnsureRegistered()` registers all message types with `polymorphic.RegisterType[T]()` via `init()`

### Client Layer (`pkg/client/`)

Public SDK. The `Client` interface provides:

```go
type Client interface {
    GetSpaces(ctx, storeID) Enumerator[string]
    GetSegments(ctx, storeID, space) Enumerator[string]
    Peek(ctx, storeID, space, segment) (*Entry, error)
    Consume(ctx, storeID, *Consume) Enumerator[*Entry]
    ConsumeSpace(ctx, storeID, *ConsumeSpace) Enumerator[*Entry]
    ConsumeSegment(ctx, storeID, *ConsumeSegment) Enumerator[*Entry]
    Produce(ctx, storeID, space, segment, Enumerator[*Record]) Enumerator[*SegmentStatus]
    Publish(ctx, storeID, space, segment, payload, metadata) error
    SubscribeToSpace(ctx, storeID, space, handler) (Subscription, error)
    SubscribeToSegment(ctx, storeID, space, segment, handler) (Subscription, error)
    GetSubscriptionStatus(id) *SubscriptionStatus
}
```

**Creation**:

- `NewClient(provider)` — default 30s handler timeout
- `NewClientWithHandlerTimeout(provider, timeout)` — custom handler timeout
- `NewClientWithRetryPolicy(provider, policy)` — custom retry policy

**Resilience** (`resilience.go`):

- `RetryPolicy` with `MaxAttempts`, `InitialBackoff`, `MaxBackoff`, `BackoffMultiplier`
- `DefaultRetryPolicy()`: 5 attempts, 100ms initial, 5s max, 2x multiplier
- `AggressiveRetryPolicy()`: 8 attempts, 200ms initial, 15s max, 1.5x multiplier
- `IsRetryable(err)`: classifies errors — context errors and permanent errors are never retried
- Auto-reconnect: client implements `ReconnectListener` to replay subscriptions on provider reconnect

### Event Sourcing Layer (`pkg/eskit/`)

Adapter bridging streamkit with `fgrzl/es`:

- `NewStreamStore(client) es.Store` — wraps a streamkit client as an event store
- `LoadEvents()`: consumes segment entries, unmarshals `polymorphic.Envelope` → `es.DomainEvent`
- `SaveEvents()`: validates contiguous sequences, produces records via `client.Produce()`

### Server Layer (`pkg/server/`)

- **`Node`** interface: `Handle(ctx, BidiStream)` + `Close()`. Decodes a `polymorphic.Envelope`, type-switches on message type, dispatches to store operations. Recovers from handler panics.
- **`NodeManager`** interface: `GetOrCreate(ctx, storeID) (Node, error)`, `Remove(ctx, storeID)`, `Close()`. Uses functional options pattern (`WithStoreFactory()`, `WithMessageBusFactory()`). Includes **circuit breaker**: after 3 consecutive store creation failures, blocks retries for 30s.
- **Context**: `WithChannelID(ctx, uuid)` / `ChannelIDFromContext(ctx)` — typed context keys (not string-based).

### Storage Layer (`pkg/storage/`)

`Store` interface:

```go
type Store interface {
    GetSpaces(ctx) Enumerator[string]
    GetSegments(ctx, space) Enumerator[string]
    ConsumeSpace(ctx, *ConsumeSpace) Enumerator[*Entry]
    ConsumeSegment(ctx, *ConsumeSegment) Enumerator[*Entry]
    Peek(ctx, space, segment) (*Entry, error)
    Produce(ctx, *Produce, Enumerator[*Record]) Enumerator[*SegmentStatus]
    Close()
}
```

**PebbleDB** (`pebblekit/`): Embedded local storage. Uses `lexkey.Encode()` for ordered key construction. Per-segment write locks (`sync.Map`). Expiring cache for segment inventory.

**Azure Table Storage** (`azurekit/`): Custom `HTTPTableClient` (not the Azure SDK). Batch writes (90 ops/batch, 3.75MB limit). WAL-based transaction recovery with background monitor. Parallel `AddEntity` workers.

`StoreFactory` interface: `NewStore(ctx, storeID) (Store, error)`

### Transport Layer (`pkg/transport/`)

**`BidiStream`** interface — all transports must implement:

```go
type BidiStream interface {
    Encode(any) error
    Decode(any) error
    CloseSend(error) error
    Close(error)
    EndOfStreamError() error
    Closed() <-chan struct{}
}
```

**In-Process** (`inprockit/`): Channel-based (buffered chan, size 64). Direct Go object passing with JSON fallback for type mismatches. `InProcMuxer` wires client↔server streams. Provider requires a `NodeManager`. No reconnect semantics (never disconnects).

**WebSocket** (`wskit/`): `WebSocketMuxer` multiplexes logical streams over a single `websocket.Conn`. Framed with `MuxerMsg` (ControlType + StoreID + ChannelID + Payload). Heartbeat ping/pong. Write pump with queue. `WebSocketBidiStreamProvider` manages connection lifecycle, jittered backoff reconnect, JWT auth via `fetchJWT` callback. `MuxerSession` enforces store-level authorization via scopes (`streamkit::*` or `streamkit::{storeID}`).

Server integration: `ConfigureWebSocketServer(router, nodeManager)` mounts `/streamz` endpoint using `fgrzl/mux` router with authentication/authorization middleware.

### Internal Packages

**Codec** (`internal/codec/`): Binary serialization (`EncodeEntry`/`DecodeEntry`) with Zstd compression (`EncodeEntrySnappy`/`DecodeEntrySnappy` — names are historical, implementation is Zstd via `klauspost/compress/zstd`). Little-endian binary encoding for fixed fields, length-prefixed for variable fields.

**Cache** (`internal/cache/`): `ExpiringCache` — `sync.Map` with TTL expiration. Background cleanup goroutine. PebbleDB default: 97s TTL, 59s cleanup interval.

**Transaction** (`internal/txn/`): `Transaction` struct — groups entries with TRX metadata, space/segment, sequence range, and timestamp.

## Key Patterns

### Enumerator Pattern

All list/stream operations return `enumerators.Enumerator[T]` from `fgrzl/enumerators`:

```go
entries := client.ConsumeSegment(ctx, storeID, args)
for entries.MoveNext() {
    entry, err := entries.Current()
    // process entry
}
```

`BidiStreamEnumerator[T]` bridges `BidiStream` decode loops into the enumerator interface.

### Polymorphic Message Dispatch

All API messages implement `GetDiscriminator() string` (e.g., `"streamkit://api/v1/produce"`). Messages are wrapped in `polymorphic.Envelope` for serialization. Server `Node.Handle()` type-switches on the deserialized content to dispatch operations. All message types must be registered via `polymorphic.RegisterType[T]()`.

### Factory Pattern

- `StoreFactory.NewStore(ctx, storeID)` — storage backends
- `NewClient(provider)` — client creation from `BidiStreamProvider`
- `NewInProcBidiStreamProvider(ctx, nodeManager)` — in-process transport
- `NewBidiStreamProvider(addr, fetchJWT)` — WebSocket transport

### Error Classification

`StreamsError` carries a `Code` field:

- `ErrCodeTransient` (1): temporary, retryable (e.g., `ERR_COMMIT_BATCH`, `ERR_SEQ_NUMBER_BEHIND`)
- `ErrCodePermanent` (2): terminal, do not retry (e.g., `ERR_SEQUENCE_MISMATCH`, `ERR_TRX_ID_MISMATCH`)

## Testing

**Style**: Behavioral. Arrange-Act-Assert. One behavior per test.

```go
func TestShouldProduceRecordsSuccessfullyWhenGivenValidInput(t *testing.T) {
    // Arrange
    ctx := t.Context()
    space, segment, records := "space0", "segment0", generateRange(0, 5)

    // Act
    results := harness.Client.Produce(ctx, storeID, space, segment, records)
    statuses, err := enumerators.ToSlice(results)

    // Assert
    require.NoError(t, err)
    assert.Len(t, statuses, 1)
}
```

**Rules**:

- `testify/require` in Arrange (fail fast on setup). `testify/assert` in Assert.
- Table-driven tests for input variations.
- All tests must pass with `-race` flag.
- Use `t.Context()` for context, `t.TempDir()` for temp storage.
- `test/testmain.go` sets `STREAMKIT_TEST_NO_JITTER=1` for deterministic timing.
- `test/setup_data.go`: `TestHarness`, `setupConsumerData()`, `generateRange()`.

**Integration tests** (`test/integration_test.go`) run against three configurations:

- `inproc`: PebbleDB + in-process transport
- `pebble`: PebbleDB + WebSocket transport
- `azure`: Azure Table Storage + WebSocket transport (requires Fazure emulator)

**Benchmarks**: `*_bench_test.go` files in `internal/cache/`, `internal/codec/`, `pkg/transport/inprockit/`, `pkg/transport/wskit/`.

## Dev Workflows

```bash
# Run all tests with race detector
go test -race ./...

# Run specific package
go test -race -v ./pkg/client/...

# Run single test
go test -race -run TestShouldProduceRecords ./test/...

# Run with coverage
go test -race -cover ./...

# Run benchmarks
go test -bench=. -benchmem ./internal/codec/...

# Start Azure emulator (for azure integration tests)
docker compose up -d

# Fast local integration tests
STREAMKIT_TEST_SMALL=1 go test -race ./test/...
```

The `compose.yml` runs **Fazure** (Azure Storage emulator) on ports 10000-10002 (Blob/Queue/Table).

## Dependencies

**Direct** (from `go.mod`):
| Package | Purpose |
|---|---|
| `cockroachdb/pebble/v2` | Embedded local key-value storage |
| `fgrzl/claims` | JWT claims and principal abstraction for auth |
| `fgrzl/enumerators` | Generic iterator pattern (`Enumerator[T]`) |
| `fgrzl/es` | Event sourcing interfaces (`es.Store`, `es.DomainEvent`) |
| `fgrzl/json` | Polymorphic JSON serialization (`polymorphic.Envelope`) |
| `fgrzl/lexkey` | Lexicographic key encoding for ordered storage keys |
| `fgrzl/messaging` | Message bus abstraction for segment notifications |
| `fgrzl/mux` | HTTP router with auth middleware (used by wskit server) |
| `fgrzl/timestamp` | Timestamp utilities |
| `google/uuid` | Identifiers for stores, channels, transactions |
| `klauspost/compress` | Zstd compression for entry codec |
| `stretchr/testify` | Test assertions (`require`/`assert`) |
| `golang.org/x/net` | WebSocket implementation |

## Common Task Patterns

**Adding a new API operation**:

1. Define message type in `pkg/api/messages.go` with `GetDiscriminator()` method
2. Register it in `pkg/api/registry.go` via `polymorphic.RegisterType[T]()`
3. Implement in `pkg/storage/` backends (both `pebblekit` and `azurekit`)
4. Add handler case in `pkg/server/node.go` `Handle()` switch
5. Expose via `pkg/client/client.go` `Client` interface
6. Add integration test in `test/integration_test.go` across all configurations

**Adding a transport**: Implement `BidiStream` interface, create a Muxer, implement `BidiStreamProvider` interface.

**Storage optimization**: `internal/codec/` for serialization, `internal/cache/` for caching, `pkg/storage/*/` for backend-specific tuning.

**Testing**: Use `test/setup_data.go` fixtures. Follow `test/integration_test.go` multi-backend pattern.
