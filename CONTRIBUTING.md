# Contributing to Streamkit

Thank you for your interest in contributing to Streamkit! We appreciate contributions of all kinds—bug reports, feature requests, documentation, and code.

## Code of Conduct

Be respectful, inclusive, and professional. We aim to maintain a welcoming community for everyone.

## Getting Started

### Prerequisites

- **Go 1.25.6 or later** — [Install Go](https://golang.org/doc/install)
- **Git** — [Install Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
- **Docker** (optional) — For Azure Storage emulation during testing

### Development Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/fgrzl/streamkit.git
   cd streamkit
   ```

2. **Install dependencies:**
   ```bash
   go mod download
   ```

3. **Optional: Start Azure Storage emulator** (for integration testing):
   ```bash
   docker-compose up -d
   ```

4. **Verify your setup:**
   ```bash
   go test -race ./...
   ```

## Development Workflow

### Branch Strategy

- **`develop`** — Main development branch. All PRs target this branch.
- **Feature branches** — Create from `develop` with descriptive names:
  ```bash
  git checkout -b feature/add-retry-logic
  git checkout -b fix/race-condition-in-muxer
  ```

### Commits

- Write clear, concise commit messages in the present tense:
  ```
  Add context-aware retry sleep in http_client
  Fix data race in pseudoRandState global variable
  ```
- Keep commits focused on a single logical change
- Reference issues when applicable: `Fixes #123`

### Pull Requests

1. **Before opening a PR:**
   - Run tests: `go test -race ./...`
   - Format code: `go fmt ./...`
   - Verify no new lint issues

2. **PR description should include:**
   - What problem does this solve?
   - How does it solve the problem?
   - Any breaking changes or new dependencies?
   - Link to related issues

3. **PR checklist:**
   - [ ] Tests added/modified to cover changes
   - [ ] All tests pass with `-race` flag
   - [ ] Code follows project conventions
   - [ ] Documentation updated (if applicable)
   - [ ] No new public API without docs

## Code Style and Conventions

### Go Idioms

- Follow the [Effective Go](https://golang.org/doc/effective_go) guidelines
- Use `gofmt` for formatting (enforced by CI)
- Keep functions focused and single-purpose
- Use clear, descriptive names

### Naming Conventions

- **Packages:** Lowercase, concise (`api`, `client`, `storage`, `transport`)
- **Interfaces:** End with `-er` suffix (`Reader`, `Consumable`, `Routeable`)
- **Constants:** All caps for unexported (`const maxRetries`, `const maxActiveHandlers`)
- **Types:** Public types start with capital letter

### Logging

Use `log/slog` and be **strategic about levels** so production logs stay actionable:

- **Info** — Lifecycle and important operational events only (e.g. factory initialized, first connection, reconnect successful, connection closed, enqueuing reconnection). Avoid per-request or per-message Info.
- **Warn** — Recoverable or degraded conditions (e.g. handler timeout, reconnect queue full, backoff).
- **Error** — Failures that need attention (e.g. stream creation failed, handler panic, table creation failed).
- **Debug** — Reserved for troubleshooting; keep minimal so default (Info) stays quiet.

Prefer structured fields that make correlation and triage fast. Use the identifiers that matter for the operation instead of free-form text parsing:

- `store_id`, `space`, `segment`, `channel_id` for stream and transport scope
- `request_id`, `subscription_id`, `transaction_id` for end-to-end correlation
- `attempt`, `max_attempts`, `last_sequence`, `record_count`, `entry_count` for progress and retry state
- `error_type` plus the original `error` value for grouping common failure modes

Do not add Info logs on hot paths (peek, publish, produce, per-subscription connect, per-offset retry). Prefer metrics or tracing for high-cardinality observability.
Routine reconnect success, benign disconnects, and ordinary close events belong in Debug unless they indicate a degraded state that needs operator action.

### Project Architecture

Streamkit has a clear layered architecture:

```
┌─ api:        Domain models and interfaces (Entry, Record, Space, Segment)
├─ client:     Public SDK for consuming/producing (client.Client interface)
├─ server:     Server-side routing and session management
├─ storage:    Persistence layer with multiple backends
│  ├─ pebblekit:   Local embedded storage (PebbleDB)
│  └─ azurekit:    Cloud storage (Azure Table Storage)
├─ transport:  Multiple protocol implementations
│  ├─ inprockit:   In-process (direct method calls)
│  ├─ wskit:       WebSocket with multiplexing
│  ├─ grpckit:     gRPC (future)
│  └─ http2kit:    HTTP/2 (future)
└─ internal:   Private utilities not part of public API
   ├─ codec:       Binary serialization + compression
   ├─ cache:       Expiring cache for segments
   └─ txn:         Write-Ahead Log transaction support
```

When adding features, ensure they follow this separation of concerns:
- **Core logic in `pkg/api/`** — Define interfaces first
- **Implementation in backends** — Implement storage interface, add transport
- **Expose via `pkg/client/`** — Add public API methods

## Testing

Streamkit uses behavioral tests with clear Arrange-Act-Assert structure. See [docs/test_guidelines.md](docs/test_guidelines.md) for detailed guidelines.

### Test Naming

Use **behavioral style** names that describe the assertion:

```go
func TestShouldReturnErrorWhenUserIsInvalid(t *testing.T)
func TestShouldStoreResultGivenValidInput(t *testing.T)
func TestHandlerPanicResilience(t *testing.T)  // Behavior-focused
```

### Test Structure

```go
func TestShouldConsumeSegmentWithOffset(t *testing.T) {
	// Arrange — Set up test fixtures and dependencies
	require.NoError(t, err)  // Use require to fail fast during setup
	store := setupTestStore(t)
	defer store.Close()

	// Act — Perform the action being tested
	result, err := store.ConsumeSegment(ctx, "space", "segment")

	// Assert — Verify the outcome
	assert.NoError(t, err)  // Use assert for actual test assertions
	assert.Equal(t, expected, result)
}
```

### Running Tests

```bash
# All tests with race detector
go test -race ./...

# Specific package with verbose output
go test -race -v ./pkg/client/...

# Single test
go test -race -run TestShouldConsumeSegment ./pkg/client/...

# With coverage
go test -race -cover ./...

# With timeout (default 10m)
go test -race -timeout=2m ./...
```

### Test Requirements

- ✅ All new code must have tests
- ✅ Tests must pass with `-race` flag (no data races)
- ✅ Each test should test **one behavior only**
- ✅ Use table-driven tests for input variations
- ✅ Mock external dependencies (WebSocket, Azure, etc.)

## Documentation

### Code Comments

- **Exported functions/types:** Add a comment starting with the declaration name
  ```go
  // Consume reads entries from the stream until the context is canceled.
  func (s *Stream) Consume(ctx context.Context) error { ... }
  ```

- **Non-obvious logic:** Explain the "why", not the "what"
  ```go
  // Issue #3: Use per-segment locking to prevent concurrent writes
  // from overwriting each other in the same segment.
  mu := s.getLock(segmentKey)
  ```

### Issue References

When fixing a known issue, reference it in code comments:
```go
// Issue #15: Set sentinel header so token failures are visible
req.Header.Set("X-Auth-Failure", "token_acquisition_failed")
```

### CHANGELOG

Document significant changes in [CHANGELOG.md](CHANGELOG.md) under `[Unreleased]`:

```markdown
### Fixed
- Client: Fixed data race in pseudoRandState (#123)

### Added
- Storage: New caching layer for frequently accessed segments

### Changed
- Transport: Use context for WebSocket cancellation
```

## Building and Running

### Build

```bash
go build ./...
```

### Run Examples

Streamkit is currently in **alpha** and doesn't have public binaries yet. Use it as a library in your Go applications.

```go
import "github.com/fgrzl/streamkit"
```

## Reporting Issues

### Bug Reports

Include:
- Go version: `go version`
- Streamkit version: Git commit hash or local `go.mod`
- Steps to reproduce
- Expected vs. actual behavior
- Relevant logs or stack traces

Example:
```
**Description:** WebSocket connection fails on startup with nil pointer panic

**Steps:**
1. Create client with wskit provider
2. Call client.Consume()
3. Observe panic in server.go connect()

**Expected:** Graceful 401 Unauthorized response

**Actual:** Panic in session.CanAccessStore()

**Logs:**
```
panic: runtime error: invalid memory address or nil pointer dereference [recovered]
    at main.(*webSocketServer).connect (server.go:35)
```

### Feature Requests

Include:
- Use case — what would this enable?
- Proposed API design (if applicable)
- Examples of how users would use this feature

## Concurrency and Safety

Streamkit is designed for high-throughput concurrent access. When contributing:

- ✅ Always run tests with `-race` flag
- ✅ Use `sync.Mutex`, `sync.Map`, or `atomic.Int*` for shared state
- ✅ Avoid global mutable variables
- ✅ Document which fields require synchronization
- ✅ Prefer channels for goroutine coordination

Example of proper synchronization:
```go
// Good: atomic access to shared counter
var activeHandlers atomic.Int32

// Good: synchronized access to lock map
var segLocks sync.Map

// ❌ Bad: unsynchronized global variable
var globalState = 0  // Data race!
```

## Performance and Benchmarks

For performance-critical paths (codec, muxer, cache), include benchmarks:

```bash
# Run benchmarks
go test -bench=. ./internal/codec/...

# Compare against baseline
go test -bench=. -benchmem ./internal/codec/...
```

Benchmarks should be in `*_bench_test.go` files.

## Alpha Testing and Known Limitations

Streamkit is currently **alpha**. Contributors should be aware of:

1. **Lock Map Growth** — Per-segment produce locks persist. Not an issue for <10K segments.
2. **Deduplication** — Implement sequence-based deduplication for exactly-once semantics.
3. **Subscription Failures** — Monitor subscription health before failures occur.

See [CHANGELOG.md](CHANGELOG.md) for detailed limitations and planned mitigations.

## Becoming a Maintainer

Active contributors with a track record of quality contributions may be invited to become maintainers. This includes:

- Code review authority
- Merge privileges
- Issue triage
- Release management

## Questions?

- Check [README.md](README.md) for architecture overview
- Read [docs/test_guidelines.md](docs/test_guidelines.md) for testing details
- Review [CHANGELOG.md](CHANGELOG.md) for recent changes and known issues
- Open a discussion or issue on GitHub

Thank you for helping make Streamkit better! 🚀
