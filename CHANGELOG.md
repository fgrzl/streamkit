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

### Known Limitations

#### Client (pkg/client)
1. **Lock Map Growth**: Per-segment produce locks accumulate over time without cleanup. 
   - **Impact**: Not an issue for typical workloads (<10,000 active segments), but high-churn 
     scenarios (millions of unique segments) may see gradual memory growth.
   - **Mitigation**: Document as known limitation for alpha. Will consider LRU eviction or 
     periodic cleanup in beta if needed.

2. **Consumption Deduplication**: While resilience enumerators now resume from last consumed 
   position, consumers should still implement deduplication based on sequence numbers as a 
   best practice for exactly-once semantics.
   - **Impact**: In rare edge cases (concurrent producers, clock skew), duplicate entries 
     may still occur.
   - **Mitigation**: Applications should track sequence numbers and deduplicate at application 
     level for critical workflows.

3. **Subscription Lifecycle**: Retryable subscription failures stay in the reconnect loop, while
   permanent errors stop the subscription and remove it from the client registry.
   - **Impact**: Applications should treat permanent subscription errors as terminal unless they
     explicitly resubscribe.
   - **Best Practice**: Surface subscription lifecycle through application logs and metrics, and
     treat reconnect deliveries as `latest snapshot -> live updates`.

---

## Future Releases

### Planned for Beta
- Lock map cleanup/LRU eviction for high-churn segment scenarios
- Subscription failure callback: `OnSubscriptionFailed(id string, err error)`
- Enhanced observability: Prometheus metrics for client operations
- Batch API for high-throughput produces (Issue 10)

### Planned for RC
- Expand live reconnect coverage for consume paths and operational guidance
- Performance benchmarks and optimization
- Production deployment guide
- Example applications and patterns

---

## Version History

_No releases yet. Initial alpha release coming soon._
