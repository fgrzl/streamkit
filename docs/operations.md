# Operations

Running Streamkit in production: observability, deployment, and day-two operations. For guarantees, checklists, and failure playbooks, start with [Production](production.md).

## Local development

```bash
go mod download
go test -race ./...                    # full suite
STREAMKIT_TEST_SMALL=1 go test -race ./test/...  # faster integration subset
docker compose up -d                   # Fazure / Azurite for azure tests
```

Integration tests use three configurations (`test/harness_test.go`):

| Name | Storage | Transport |
|------|---------|-----------|
| `inproc` | Pebble temp dir | inprockit |
| `pebble` | Pebble temp dir | WebSocket + JWT |
| `azure` | Fazure tables | WebSocket + JWT |

Set `STREAMKIT_TEST_NO_JITTER=1` in `test/testmain.go` for deterministic timing in CI.

## Azure emulator

`compose.yml` runs [Fazure](https://github.com/fgrzl/fazure) on:

| Port | Service |
|------|---------|
| 10000 | Blob |
| 10001 | Queue |
| 10002 | Table |

Azure-backed tests expect table endpoint `http://127.0.0.1:10002/devstoreaccount1`. Production uses your Azure Storage account endpoint with TLS.

## Telemetry

`pkg/telemetry` provides OpenTelemetry tracing and metrics helpers used by client and `wskit` (queue depth, etc.). Wire exporters in your application’s OTel SDK setup.

Client tracing: `pkg/client/tracing.go`. Prefer structured `log/slog` with fields documented in [CONTRIBUTING](../CONTRIBUTING.md) — avoid Info logs on hot paths.

**Production alerts (recommended):**

| Metric / signal | Indicates |
|-----------------|-----------|
| WebSocket reconnect rate | Network, auth, or server instability |
| `ErrSequenceMismatch` rate | Producer coordination bugs |
| Subscription permanent failures | Auth, scope, or handler panics |
| Store creation circuit open | Storage backend unavailable |
| Handler timeout count | Slow subscription callbacks |

## WebSocket chaos soak (optional)

Local stress test (not run in default CI):

```bash
STREAMKIT_SOAK=1 STREAMKIT_SOAK_DURATION=90s \
  go test -run TestLocalWebSocketChaosSoak -count=1 ./pkg/transport/wskit
```

Requires Fazure if using Azure factory in the soak test.

## Production deployment

```
                    ┌──────────────┐
  Clients ──WSS──►  │ Load balancer │
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
         API instance   API instance   API instance
         mux + wskit    mux + wskit    mux + wskit
         NodeManager    NodeManager    NodeManager
              │            │            │
              └────────────┼────────────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
         azurekit      Message bus    JWT / identity
         (shared)       (required for   provider
                        subscriptions)
```

### Multi-instance requirements

| Feature | Single instance | Multiple instances |
|---------|-----------------|-------------------|
| Produce / consume / peek | Pebble or Azure | **Azure** (shared storage) |
| Subscriptions | Optional bus | **Shared message bus** required |
| Pebble storage | Supported | **Not supported** (disk is local to one host) |

### Configuration

- **Store per tenant** — UUID in JWT scope and storage isolation.
- **Message bus** — Must deliver `SegmentNotification` to all nodes serving subscribers for that store/space.
- **Idle eviction** — `WithIdleEviction` on node manager to bound open stores on busy hosts.
- **Backups** — Pebble: volume snapshots; Azure: platform backup and geo-redundancy per your policy.
- **TLS** — Terminate at load balancer or app; clients use `wss://`.

## Health checks

When using `fgrzl/mux`, expose `router.Healthz().AllowAnonymous()` for load balancer probes (see test harness). Health checks validate process readiness, not storage depth—add separate storage probes if needed.

## CI and releases

GitHub Actions workflows: `ci.yml` (test/lint), `publish.yml`, `version.yml`. All tests pass with `-race` before release tags are published to the Go module proxy.

## Logging levels

| Level | Use |
|-------|-----|
| Info | Lifecycle: listen, connect, reconnect success |
| Warn | Degraded: handler timeout, queue pressure |
| Error | Failures needing action |
| Debug | Per-stream diagnostics only |

See CONTRIBUTING for field naming (`store_id`, `space`, `segment`, `channel_id`).
