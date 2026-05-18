# Streamkit documentation

Streamkit is a **production-ready**, high-throughput hierarchical event streaming platform for Go. Data is organized in **stores**, **spaces**, and **segments**, with pluggable storage backends and transports.

**Module:** `github.com/fgrzl/streamkit` — import `pkg/client`, `pkg/server`, `pkg/storage/*`, and `pkg/transport/*` (there is no root `streamkit` package).

## Production

| Document | Description |
|----------|-------------|
| **[Production](production.md)** | Deployment stack, guarantees, checklists, failure playbooks |
| [Operational contracts](limitations.md) | Delivery semantics, subscriptions, and application responsibilities |
| [Operations](operations.md) | Telemetry, multi-instance deployment, health checks |

## Start here

| Document | Description |
|----------|-------------|
| [Overview](overview.md) | What Streamkit is, design goals, stability |
| [Core concepts](concepts.md) | Stores, spaces, segments, entries, offsets, and transactions |
| [Architecture](architecture.md) | Layers, request flow, and package layout |
| [Getting started](getting-started.md) | Minimal local setup and first produce/consume cycle |

## Guides

| Document | Description |
|----------|-------------|
| [Client guide](client-guide.md) | Public SDK: consume, produce, subscriptions, leases, resilience |
| [Server guide](server-guide.md) | Nodes, node manager, routing, and subscriptions |
| [API protocol](api-protocol.md) | Messages, discriminators, errors, and polymorphic envelopes |
| [Storage](storage.md) | PebbleDB and Azure Table backends, keys, and transactions |
| [Transport](transport.md) | In-process and WebSocket transports, auth, and multiplexing |
| [Event sourcing](event-sourcing.md) | `eskit` adapter for `fgrzl/es` |

## Reference

| Document | Description |
|----------|-------------|
| [Test guidelines](test_guidelines.md) | How we write and run tests in this repository |

## Related

- [README](../README.md) — project entry point
- [CHANGELOG](../CHANGELOG.md) — release notes and roadmap
- [CONTRIBUTING](../CONTRIBUTING.md) — contribution workflow
