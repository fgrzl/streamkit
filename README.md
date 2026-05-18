[![CI](https://github.com/fgrzl/streamkit/actions/workflows/ci.yml/badge.svg)](https://github.com/fgrzl/streamkit/actions/workflows/ci.yml)
[![Publish](https://github.com/fgrzl/streamkit/actions/workflows/publish.yml/badge.svg)](https://github.com/fgrzl/streamkit/actions/workflows/publish.yml)

# Streamkit

High-throughput, hierarchical event streaming for Go. Organize data in **stores**, **spaces**, and **segments**; consume with strict per-segment ordering or timestamp-merged space views; plug in PebbleDB or Azure Table Storage and in-process or WebSocket transport.

**Production-ready** — used in live services. See [docs/production.md](docs/production.md) for deployment guidance and [docs/limitations.md](docs/limitations.md) for operational contracts (delivery semantics, cursors, subscriptions).

## Documentation

Full documentation: **[docs/](docs/README.md)**

| Topic | Guide |
|-------|--------|
| **Production** | [docs/production.md](docs/production.md) |
| Concepts (store / space / segment) | [docs/concepts.md](docs/concepts.md) |
| Architecture and packages | [docs/architecture.md](docs/architecture.md) |
| Getting started | [docs/getting-started.md](docs/getting-started.md) |
| Client SDK | [docs/client-guide.md](docs/client-guide.md) |
| Server embedding | [docs/server-guide.md](docs/server-guide.md) |
| Storage backends | [docs/storage.md](docs/storage.md) |
| Transport (in-proc / WebSocket) | [docs/transport.md](docs/transport.md) |
| Event sourcing (`eskit`) | [docs/event-sourcing.md](docs/event-sourcing.md) |
| Operations | [docs/operations.md](docs/operations.md) |

## Quick example

```go
import (
    "context"
    "github.com/fgrzl/enumerators"
    "github.com/fgrzl/streamkit/pkg/client"
    "github.com/fgrzl/streamkit/pkg/server"
    "github.com/fgrzl/streamkit/pkg/storage/pebblekit"
    "github.com/fgrzl/streamkit/pkg/transport/inprockit"
    "github.com/google/uuid"
)

func main() {
    ctx := context.Background()
    factory, _ := pebblekit.NewStoreFactory(&pebblekit.PebbleStoreOptions{Path: "./data"})
    nm := server.NewNodeManager(server.WithStoreFactory(factory))
    provider := inprockit.NewInProcBidiStreamProvider(ctx, nm)
    c := client.NewClient(provider)
    defer c.Close()

    storeID := uuid.New()
    records := enumerators.Slice([]*client.Record{{Sequence: 1, Payload: []byte("hello")}})
    _, _ = enumerators.ToSlice(c.Produce(ctx, storeID, "demo", "events", records))
}
```

See [Getting started](docs/getting-started.md) for a complete produce/consume walkthrough. For WebSocket + JWT production wiring, see [Transport](docs/transport.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and [docs/test_guidelines.md](docs/test_guidelines.md). Changelog: [CHANGELOG.md](CHANGELOG.md).

## License

See [LICENSE](LICENSE).
