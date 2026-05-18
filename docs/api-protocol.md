# API protocol

Wire protocol types are defined in `github.com/fgrzl/streamkit/pkg/api`. Clients and servers exchange **polymorphic envelopes** over a `BidiStream`.

## BidiStream

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

`BidiStreamProvider` opens a stream for a store:

```go
CallStream(ctx context.Context, storeID uuid.UUID, routeable Routeable) (BidiStream, error)
```

The first encoded message is the routeable request; subsequent messages depend on the operation (streaming entries, status updates, etc.).

## Message catalog

| Message | Discriminator | Role |
|---------|---------------|------|
| `Produce` | `streamkit://api/v1/produce` | Start produce; stream `Record` in, `SegmentStatus` out |
| `Peek` | `streamkit://api/v1/peek` | Latest entry |
| `Consume` | `streamkit://api/v1/consume` | Multi-space consume |
| `ConsumeSpace` | `streamkit://api/v1/consume_space` | Space-wide consume |
| `ConsumeSegment` | `streamkit://api/v1/consume_segment` | Segment consume |
| `GetSpaces` | `streamkit://api/v1/get_spaces` | List spaces |
| `GetSegments` | `streamkit://api/v1/get_segments` | List segments |
| `GetStatus` | `streamkit://api/v1/get_status` | Store status |
| `SubscribeToSegmentStatus` | `streamkit://api/v1/subscribe_to_segment_status` | Live status stream |
| `SegmentStatus` | `streamkit://api/v1/segment_status` | Status payload |
| `SegmentNotification` | `streamkit://api/v1/segment_notification` | Bus notification |
| `LeaseAcquire` / `LeaseRenew` / `LeaseRelease` | `streamkit://api/v1/lease_*` | Distributed leases |
| `LeaseResult` | `streamkit://api/v1/lease_result` | Lease operation result |

Registration: types are registered in `pkg/api/registry.go` via `init` → `EnsureRegistered()`. You do not need to call `EnsureRegistered()` in application code unless you import `api` without going through packages that already trigger `init`.

## Routeable and Consumable

- `Routeable` — marker for messages passed to `CallStream` (`Produce`, `Peek`, `ConsumeSegment`, …).
- `Consumable` — optional marker for types that implement `GetSpaces() []string` for space routing hints. Multi-space `Consume` routing is driven primarily by the `Offsets` map keys in the request body.

## Errors

Errors use `StreamsError` with a code:

| Code | Meaning | Retry |
|------|---------|-------|
| `ErrCodeTransient` (1) | Temporary failure | Yes (with policy) |
| `ErrCodePermanent` (2) | Terminal | No |

Predefined variables include:

| Error | Typical cause |
|-------|----------------|
| `ErrSequenceMismatch` | Permanent sequence conflict |
| `ErrSeqNumberBehind` / `ErrSeqNumberAhead` | Transient sequence skew |
| `ErrTrxIDMismatch` | Transaction ID conflict |
| `ErrTrxNumberAhead` / `ErrTrxNumberBehind` | Transaction number conflict |
| `ErrCommitBatch` | Storage batch failure (retry) |

Use `errors.As` to inspect `*api.StreamsError` and the `Code` field.

## Entry metadata

```go
type TRX struct {
    ID     uuid.UUID
    Node   uuid.UUID
    Number uint64
}
```

Every `Entry` carries `TRX` linking it to the write transaction.

## Extending the protocol

To add an operation:

1. Add struct + `GetDiscriminator()` in `pkg/api/messages.go`.
2. Register in `pkg/api/registry.go`.
3. Implement on `pebblekit` and `azurekit` stores.
4. Add case in `pkg/server/node.go` `Handle`.
5. Expose on `pkg/client.Client`.
6. Add integration tests in `test/*_integration_test.go`, looping `configurations()` for `inproc`, `pebble`, and `azure`.

See [Architecture](architecture.md) and [Test guidelines](test_guidelines.md).
