# Architecture

Streamkit follows a layered design: domain types and protocol in `pkg/api`, persistence in `pkg/storage`, routing in `pkg/server`, public SDK in `pkg/client`, and protocol implementations in `pkg/transport`.

## Layer diagram

```
┌─────────────────────────────────────────────────────────────┐
│  Application                                                 │
│  (your service — uses pkg/client)                            │
└───────────────────────────┬─────────────────────────────────┘
                            │ BidiStreamProvider
┌───────────────────────────▼─────────────────────────────────┐
│  Transport: inprockit │ wskit (WebSocket /streamz)          │
└───────────────────────────┬─────────────────────────────────┘
                            │ BidiStream (encode/decode)
┌───────────────────────────▼─────────────────────────────────┐
│  Server: NodeManager → Node.Handle                           │
│  (polymorphic dispatch, subscriptions, leases)               │
└─────────────┬─────────────────────────────┬───────────────────┘
              │                             │
┌─────────────▼─────────────┐   ┌───────────▼───────────┐
│  Storage: Store            │   │  Bus: segment         │
│  pebblekit │ azurekit       │   │  notifications        │
└─────────────┬─────────────┘   └───────────────────────┘
              │
┌─────────────▼─────────────┐
│  internal: codec, cache,   │
│  txn, lease                │
└────────────────────────────┘
```

## Package layout

| Package | Responsibility |
|---------|----------------|
| `pkg/api` | Messages, `Entry`/`Record`, errors, `BidiStream`, registry |
| `pkg/client` | Public `Client` interface, resilience, subscriptions |
| `pkg/server` | `Node`, `NodeManager`, subscription routing |
| `pkg/storage` | `Store` / `StoreFactory` interfaces |
| `pkg/storage/pebblekit` | Embedded PebbleDB |
| `pkg/storage/azurekit` | Azure Table Storage (custom HTTP client) |
| `pkg/transport/inprockit` | In-process muxer (tests, local) |
| `pkg/transport/wskit` | WebSocket muxer, JWT session, server mount |
| `pkg/bus` | In-process notification routing |
| `pkg/eskit` | `fgrzl/es` event store adapter |
| `pkg/telemetry` | OpenTelemetry helpers |
| `internal/codec` | Binary entry encoding + Zstd compression |
| `internal/cache` | TTL cache for segment inventory |
| `internal/txn` | Transaction grouping for writes |
| `internal/lease` | Lease store for `WithLease` |
| `internal/telemetry` | Test/dev OTel SDK initialization helpers |

Additional transports (gRPC, HTTP/2) are not implemented; only `inprockit` and `wskit` are available today.

## Request flow

1. **Client** opens a logical stream via `BidiStreamProvider.CallStream(ctx, storeID, routeable)`.
2. The client sends a **polymorphic envelope** containing the first message (`Produce`, `ConsumeSegment`, etc.).
3. **Transport** delivers frames to the server **Node** for that `storeID`.
4. **Node.Handle** decodes envelopes in a loop, dispatches to `storage.Store`, and encodes responses on the same `BidiStream`.
5. For streaming operations, multiple response messages are sent until the enumerator completes or the stream closes.

Each logical operation typically uses one bidirectional stream. The WebSocket transport **multiplexes** many logical streams over a single physical connection.

## Node manager

`server.NewNodeManager` accepts:

- `WithStoreFactory` — creates a `storage.Store` per store UUID
- `WithMessageBusFactory` — enables segment status notifications to subscribers
- `WithIdleEviction` — optional closure of idle store nodes

A **circuit breaker** blocks store creation after repeated failures (3 failures → 30s cooldown).

## Enumerators

List and stream APIs return `enumerators.Enumerator[T]` from `github.com/fgrzl/enumerators`:

```go
for enum.MoveNext() {
    item, err := enum.Current()
    // handle item
}
```

`api.NewStreamEnumerator` adapts a `BidiStream` decode loop into an enumerator.

## Polymorphic protocol

All API messages implement `GetDiscriminator() string` (for example `streamkit://api/v1/produce`). Types are registered in `pkg/api/registry.go` via `polymorphic.RegisterType`. Wire format uses `polymorphic.Envelope` from `github.com/fgrzl/json`.

## Separation of concerns

| Concern | Where it lives |
|---------|----------------|
| What operations exist | `pkg/api` messages + `pkg/client` |
| How requests are routed | `pkg/server` |
| How data is stored | `pkg/storage/*` |
| How bytes move | `pkg/transport/*` |

Adding an operation requires: message type → registry → storage backends → server switch → client method → tests across harness configurations (see [Test guidelines](test_guidelines.md)).
