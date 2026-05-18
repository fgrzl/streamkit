# Changelog

All notable changes to the Streamkit project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **Client:** `NewClientWithRetryPolicy()` now correctly uses the provided retry policy instead of ignoring it
- **Client:** Added ±25% jitter to exponential backoff to prevent thundering herd on mass reconnects
- **Client:** Improved `IsRetryable()` to exclude permanent errors (auth failures, not found, invalid arguments)
- **Client:** Enhanced offset resume in resilience enumerator:
  - `ConsumeSpace` now increments timestamp by 1ns to avoid duplicate entries
  - `Consume` now updates offset map with last seen sequence per space/segment pair
  - Added debug logging for offset updates

### Documentation
- Production documentation: [docs/production.md](docs/production.md), hardened operational contracts in [docs/limitations.md](docs/limitations.md)

### Operational contracts

Documented production behavior (see [docs/limitations.md](docs/limitations.md)):

#### Client (pkg/client)
1. **Lock map growth** — Per-segment produce locks accumulate without automatic eviction.
   - **Impact:** Not an issue for typical workloads (&lt;10,000 active segments); high-churn scenarios (millions of unique segment keys) may see gradual memory growth.
   - **Mitigation:** Monitor segment cardinality; recycle long-lived client processes if needed. LRU eviction is on the roadmap.

2. **Consumption deduplication** — Delivery is at-least-once; resilience enumerators resume from last consumed position when possible.
   - **Impact:** Rare duplicates under concurrent producers or clock skew.
   - **Mitigation:** Dedupe on `Entry.Sequence` at the application layer for exactly-once business semantics.

3. **Subscription lifecycle** — Retryable failures stay in the reconnect loop; permanent errors stop the subscription and remove it from the client registry.
   - **Impact:** Applications must resubscribe after permanent errors.
   - **Contract:** Reconnect delivers `latest snapshot → live updates`. Durable replay/cursors are not part of the subscription API.

---

## Roadmap

- Lock map cleanup/LRU eviction for high-churn segment scenarios
- Subscription failure callback: `OnSubscriptionFailed(id string, err error)`
- Enhanced observability: Prometheus metrics for client operations
- Batch API for high-throughput produces
- Expanded consume reconnect guidance and benchmarks

---

## Version History

See [GitHub Releases](https://github.com/fgrzl/streamkit/releases) for tagged versions published to the Go module proxy.
