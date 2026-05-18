# Overview

Streamkit provides **ordered, hierarchical event streams** with a clear separation between logical organization (spaces and segments) and physical persistence (storage backends). Clients interact through a stable Go SDK; servers route requests to per-store nodes backed by PebbleDB or Azure Table Storage.

**Streamkit is production-ready** — it runs in live environments today as an embedded library. See [Production](production.md) for deployment guidance and [Operational contracts](limitations.md) for delivery semantics your application must handle.

## Design goals

- **Hierarchical streams** — Group related data in spaces; preserve strict per-segment ordering while allowing parallel consumption across segments.
- **Pluggable storage** — Same API over embedded PebbleDB (local) or Azure Table Storage (cloud).
- **Pluggable transport** — In-process channels for tests; WebSocket multiplexing for production traffic.
- **At-least-once delivery** — Consumers track offsets or sequences; applications deduplicate when exactly-once semantics matter.
- **Observable** — OpenTelemetry tracing and metrics hooks in client and transport layers.

## What Streamkit is

Streamkit is a **library**, not a standalone daemon. You embed it in your application:

1. Choose a **storage factory** (`pebblekit` or `azurekit`).
2. Create a **node manager** that opens stores on demand.
3. Expose a **transport** (typically WebSocket via `wskit`) or use **in-process** transport for tests.
4. Connect with the **client** (`pkg/client`) from application or service code.

## What Streamkit is not

- Not a managed cloud product — you host storage and the HTTP/WebSocket endpoint.
- Not Kafka or Pulsar — no broker cluster, partitions, or consumer groups in the Kafka sense. Segments provide ordering; spaces provide grouping and cross-segment reads.
- Not an exactly-once platform by itself — delivery is at-least-once; see [Operational contracts](limitations.md).

## Typical use cases

- **Event sourcing** — Persist domain events per aggregate (segment) via [`eskit`](event-sourcing.md).
- **Audit and activity logs** — Append-only segments with timestamp-ordered space views.
- **Real-time inventory** — Subscribe to segment status updates when head sequence or timestamps change.
- **Multi-tenant apps** — One store UUID per tenant; spaces isolate domains within a tenant.

## Stability

- **Go module:** `github.com/fgrzl/streamkit` — consume via tagged releases (`go get ...@vX.Y.Z`).
- **Wire protocol:** Message discriminators are versioned (`streamkit://api/v1/...`). Breaking changes will introduce new discriminator versions, not silent behavior changes.
- **Changes:** Review [CHANGELOG](../CHANGELOG.md) on every upgrade.
