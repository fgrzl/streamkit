# Getting started

This guide walks through a minimal in-process setup suitable for learning and unit tests.

For **production** deployments (WebSocket, JWT, Azure or Pebble, horizontal scale), read [Production](production.md) first, then [Transport](transport.md) and [Storage](storage.md).

## Prerequisites

- Go 1.25.6 or later
- Clone: `git clone https://github.com/fgrzl/streamkit.git`

## In-process example

The fastest path wires Pebble storage, an in-process transport, and the client SDK:

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/fgrzl/streamkit/pkg/server"
	"github.com/fgrzl/streamkit/pkg/storage/pebblekit"
	"github.com/fgrzl/streamkit/pkg/transport/inprockit"
	"github.com/google/uuid"
)

func main() {
	ctx := context.Background()

	factory, err := pebblekit.NewStoreFactory(&pebblekit.PebbleStoreOptions{Path: "./data"})
	if err != nil {
		log.Fatal(err)
	}

	nodeManager := server.NewNodeManager(server.WithStoreFactory(factory))
	provider := inprockit.NewInProcBidiStreamProvider(ctx, nodeManager)
	c := client.NewClient(provider)
	defer func() {
		_ = c.Close()
		nodeManager.Close()
	}()

	storeID := uuid.New()
	space, segment := "demo", "events"

	// Produce sequences 1–3
	records := enumerators.Slice([]*client.Record{
		{Sequence: 1, Payload: []byte("a")},
		{Sequence: 2, Payload: []byte("b")},
		{Sequence: 3, Payload: []byte("c")},
	})
	statuses, err := enumerators.ToSlice(c.Produce(ctx, storeID, space, segment, records))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("produced through sequence %d\n", statuses[len(statuses)-1].LastSequence)

	// Consume from sequence 2
	args := &client.ConsumeSegment{Space: space, Segment: segment, MinSequence: 2}
	entries, err := enumerators.ToSlice(c.ConsumeSegment(ctx, storeID, args))
	if err != nil {
		log.Fatal(err)
	}
	for _, e := range entries {
		fmt.Printf("seq=%d payload=%s\n", e.Sequence, e.Payload)
	}
}
```

Run with `go run .` from your module. The `./data` directory holds the Pebble database.

## WebSocket example (outline)

For network access:

1. Create `pebblekit.NewStoreFactory` or `azurekit.NewStoreFactory`.
2. `nodeManager := server.NewNodeManager(server.WithStoreFactory(factory), server.WithMessageBusFactory(...))`.
3. Mount `wskit.ConfigureWebSocketServer(router, nodeManager)` on `/streamz` with `fgrzl/mux` authentication.
4. `provider := wskit.NewBidiStreamProvider("ws://host", fetchJWT)`.
5. `client.NewClient(provider)`.

See `test/harness_test.go` (`wskitTestHarness`) for a complete test server with JWT.

## Discover inventory

```go
spaces, _ := enumerators.ToSlice(c.GetSpaces(ctx, storeID))
segments, _ := enumerators.ToSlice(c.GetSegments(ctx, storeID, "demo"))
latest, err := c.Peek(ctx, storeID, "demo", "events")
```

## Publish shortcut

For a single record without building an enumerator:

```go
err := c.Publish(ctx, storeID, space, segment, []byte("payload"), map[string]string{"key": "value"})
```

`Publish` resolves the next sequence via peek internally.

## Next steps

- [Production](production.md) — deployment checklist and failure playbooks
- [Core concepts](concepts.md) — offsets, spaces vs segments, subscriptions
- [Client guide](client-guide.md) — resilience, subscriptions, leases
- [Transport](transport.md) — WebSocket auth and scopes
- [Event sourcing](event-sourcing.md) — `eskit` with `fgrzl/es`
