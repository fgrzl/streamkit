# Storage

Persistence is abstracted by `github.com/fgrzl/streamkit/pkg/storage`. Each **store UUID** gets its own `Store` instance from a `StoreFactory`.

## Store interface

```go
type Store interface {
    GetSpaces(ctx) enumerators.Enumerator[string]
    GetSegments(ctx, space) enumerators.Enumerator[string]
    ConsumeSpace(ctx, *api.ConsumeSpace) enumerators.Enumerator[*api.Entry]
    ConsumeSegment(ctx, *api.ConsumeSegment) enumerators.Enumerator[*api.Entry]
    Peek(ctx, space, segment) (*api.Entry, error)
    Produce(ctx, *api.Produce, records) enumerators.Enumerator[*api.SegmentStatus]
    Close()
}
```

Optional capability `SegmentStatusStore` allows fetching status without replaying the segment.

## PebbleDB (`pebblekit`)

**Use for:** local development, single-node deployments, tests.

```go
factory, err := pebblekit.NewStoreFactory(&pebblekit.PebbleStoreOptions{
    Path: "/var/lib/streamkit",
})
```

Characteristics:

- Embedded **CockroachDB Pebble** database per store path
- Keys built with `lexkey.Encode` for ordered scans (space by timestamp, segment by sequence)
- Per-segment write locks (`sync.Map`) to serialize concurrent produces
- **Expiring cache** for segment inventory (default ~97s TTL) to accelerate inventory operations

Data layout uses prefixes from [Core concepts](concepts.md): `DAT`, `INV`, `TXN`, `SPACES`, `SEGMENTS`.

## Azure Table Storage (`azurekit`)

**Use for:** cloud deployments, horizontal storage, integration with Azure ecosystem.

```go
factory, err := azurekit.NewStoreFactory(ctx, &azurekit.AzureStoreOptions{
    Prefix:            "myapp",
    AccountName:       "account",
    AccountKey:        "key",
    Endpoint:          "https://....table.core.windows.net",
    AllowInsecureHTTP: false, // true only for Azurite / Fazure emulator
})
```

Characteristics:

- Custom **HTTP table client** (not the official Azure SDK) for batching control
- Batch writes respect Azure limits (90 operations, 3.75 MB per batch)
- **WAL-based transaction recovery** with a background monitor for pending transactions
- Parallel `AddEntity` workers for throughput

Local emulator: run `docker compose up -d` (Fazure on ports 10000–10002). Tests use `http://127.0.0.1:10002/devstoreaccount1` with the standard Azurite account key.

## Produce semantics

1. Client sends records with expected sequences.
2. Store validates sequence continuity and transaction state.
3. Entries are written to segment and space indexes; inventory and status metadata are updated.
4. `SegmentStatus` is returned (and notifications sent if a bus is configured).

Concurrent producers to the same segment: one succeeds; others may receive sequence or transaction errors. Tests in `test/core_integration_test.go` assert single-winner behavior under conflict.

## Transactions

`internal/txn` groups entries with shared `TRX` metadata. Azure backend relies on WAL + recovery for consistency across batches. Clients see transaction errors when IDs or numbers do not align with server state.

## Codec

`internal/codec` serializes entries with length-prefixed fields and **Zstd** compression (`EncodeEntry` / `DecodeEntry`). Function names mentioning Snappy are historical; implementation uses Zstd.

## Choosing a backend

| Factor | Pebble | Azure |
|--------|--------|-------|
| Ops complexity | Low (local disk) | Azure account + tables |
| Multi-instance API tier | Single host only (local disk) | **Required** for shared storage |
| Latency | Lowest local | Network bound |
| Cost | Disk only | Storage + transactions |

Both backends must pass the same integration test matrix (`inproc`/`pebble`/`azure` configurations in `test/`).
