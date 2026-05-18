# Test guidelines

Streamkit tests are behavioral, race-safe, and run against multiple storage and transport combinations.

## Principles

- **One behavior per test** — clear name, single assertion focus.
- **Arrange–Act–Assert** — `require` in arrange (fail fast), `assert` in assert.
- **Race-free** — always `go test -race`.
- **Multi-backend** — integration tests iterate `configurations()` in `test/harness_test.go`.

## Naming

```go
func TestShouldProduceRecordsSuccessfullyWhenGivenValidInput(t *testing.T)
func TestShouldConcurrentProducersDetectConflict(t *testing.T)
```

Pattern: `TestShould{ExpectedBehavior}When{Condition}`.

## Structure

```go
func TestShouldPeekEmptySegment(t *testing.T) {
    for name, harnessFactory := range configurations() {
        t.Run(name, func(t *testing.T) {
            harness := harnessFactory(t)
            storeID := uuid.New()
            ctx := t.Context()

            entry, err := harness.Client.Peek(ctx, storeID, "space", "segment")

            require.NoError(t, err)
            assert.Equal(t, &client.Entry{Space: "space", Segment: "segment"}, entry)
        })
    }
}
```

## Harness configurations

| Key | Storage factory | Transport |
|-----|-----------------|-----------|
| `inproc` | Pebble temp | `inprockit` |
| `pebble` | Pebble temp | `wskit` + httptest JWT |
| `azure` | Fazure tables | `wskit` + httptest JWT |

Use `test/setup_data.go` helpers: `generateRange`, `setupConsumerData`, `TestHarness`.

## Environment variables

| Variable | Effect |
|----------|--------|
| `STREAMKIT_TEST_NO_JITTER=1` | Set in `test/testmain.go` — deterministic reconnect timing |
| `STREAMKIT_TEST_SMALL=1` | Smaller integration scenarios where supported |
| `STREAMKIT_SOAK=1` | Enable WebSocket chaos soak (local only) |
| `STREAMKIT_SOAK_DURATION` | Soak duration (e.g. `90s`) |

## Running tests

```bash
# All packages
go test -race ./...

# Integration package only
go test -race -v ./test/...

# Single test
go test -race -run TestShouldProduceRecordsSuccessfullyWhenGivenValidInput ./test/...

# Package unit tests
go test -race ./pkg/client/...

# Benchmarks
go test -bench=. -benchmem ./internal/codec/...
```

Azure configuration requires Fazure: `docker compose up -d`.

## Assertions

- `github.com/stretchr/testify/require` — setup and fatal preconditions
- `github.com/stretchr/testify/assert` — outcome checks

Prefer table-driven subtests for input variations.

## Adding coverage for new API operations

1. Unit tests in the implementing package where logic is isolated.
2. Integration test in `test/` looping `configurations()`.
3. If behavior differs by backend, assert accordingly or skip with `t.Skip` and a clear message.

## Benchmarks

Place in `*_bench_test.go` next to the optimized code (`internal/codec`, `inprockit`, `wskit`). Report with `-benchmem` when comparing allocations.

## CI expectations

- All tests pass with `-race`
- `golangci-lint` per `.golangci.yml`
- No new data races or flaky timing (use `STREAMKIT_TEST_NO_JITTER` in test main)

For contribution workflow, see [CONTRIBUTING](../CONTRIBUTING.md).
