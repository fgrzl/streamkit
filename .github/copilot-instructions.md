# Streamkit AI Coding Agent Instructions

## 🎯 Project Overview

**Streamkit** is a high-throughput, hierarchical event streaming platform written in Go. It provides scalable, organized, and reliable data flows with three core concepts:
- **Store**: Physical separation at storage level (root container)
- **Space**: Top-level logical container for related streams
- **Segments**: Independent, ordered sub-streams within a Space (strict ordering within segment, parallel consumption across segments)

Key architectural principle: **Separation of concerns** - logical hierarchy (Space/Segment) is independent from physical storage backends.

## 🏗️ Architecture & Critical Patterns

### Core Layer Architecture
1. **API Layer** (`pkg/api/`): Defines core domain models and interfaces
   - `Entry`, `Record`, `SegmentStatus` - data models
   - `Consumable`, `Routeable` - behavioral interfaces  
   - Operations: `Consume`, `ConsumeSegment`, `ConsumeSpace`, `Peek`, `Produce`

2. **Client Layer** (`pkg/client/`): Public SDK for interacting with the platform
   - Aggregates across multiple transport backends
   - Provides `Client` interface with operations: `GetSpaces()`, `GetSegments()`, `Consume()`, `Peek()`, `Produce()`
   - Uses factory pattern (`ClientFactory`, `BidiStreamProvider`)

3. **Storage Layer** (`pkg/storage/`): Abstracted persistence with multiple backends
   - `Store` interface: operations like `ConsumeSpace()`, `ConsumeSegment()`, `Peek()`
   - Implementations: **PebbleDB** (`pebblekit/`) for local storage, **Azure Tables** (`azurekit/`) for cloud
   - Encoding: Binary serialization + Snappy compression (`internal/codec/`)

4. **Transport Layer** (`pkg/transport/`): Multiple protocol implementations
   - **In-Process** (`inprockit/`): Direct method calls, no serialization
   - **WebSocket** (`wskit/`): Full-duplex bidirectional streaming with session management
   - **HTTP/2 & gRPC** (`http2kit/`, `grpckit/`): Not fully detailed but present
   - Common pattern: `BidiStream` interface for bidirectional communication + `Muxer` for channel multiplexing

5. **Server Layer** (`pkg/server/`): Message routing and node management
   - `Node`: Handles individual client connections (via `BidiStream`)
   - `Manager`: Orchestrates multiple nodes
   - Channel-level context: `WithChannelID()` / `ChannelIDFromContext()` for tracking

### Data Flow: Produce → Consume
- **Produce**: Data written to specific Segment in Space → stored via `Store.ProduceSegment()`
- **Consume Segment**: Reads entries maintaining strict ordering + offset tracking
- **Consume Space**: Reads from ALL segments interleaved, ordered by timestamp
- **Peek**: Non-consuming read of latest entry
- **Offsets & Transactions**: `internal/txn/` manages consistent multi-entry writes

### Key Interfaces to Understand
- `BidiStream`: Abstraction for bidirectional byte streams (used across all transports)
- `Enumerator[T]`: Iterator pattern from fgrzl/enumerators (implements `MoveNext()`, `Current()`)
- `Polymorphic`: Serialization abstraction from fgrzl/json for routing logic

## 🧪 Testing Conventions

**Testing Style**: Behavioral, not implementation-focused.

**Test Structure** (from `docs/test_guidelines.md`):
```go
func TestShouldReturnErrorWhenUserIsInvalid(t *testing.T) {
	// Arrange
	require.NoError(t, err)
	...
	
	// Act
	...
	
	// Assert
	assert.Equal(t, expected, actual)
}
```

**Key Rules**:
- Use `testify/require` for Arrange (fail fast), `testify/assert` for Assert
- Each test tests ONE thing only
- Table-driven tests for input variations
- Test patterns exemplified in:
  - `test/integration_test.go`: End-to-end workflows (produce/consume/spaces/segments)
  - `pkg/transport/wskit/*_test.go`: Transport-specific behaviors

## 📦 Dev Workflows & Commands

**Test Execution**:
```bash
go test ./... -v -race
```
(Shown in terminal context as recent command with exit code 0)

**Benchmarking**: Files with `_bench_test.go` exist for performance-critical paths:
- `internal/cache/cache_bench_test.go`
- `internal/codec/codec_bench_test.go`
- `pkg/transport/inprockit/bidi_stream_bench_test.go`
- `pkg/transport/wskit/bidi_stream_bench_test.go`

**Docker Setup**: `compose.yaml` runs **Fazure** (Azure Storage emulator) on ports 10000-10002:
```bash
docker-compose up -d
```
Used for testing Azure backend integration.

## 🔑 Project-Specific Patterns

1. **Enumerator Pattern**: Everywhere you iterate (GetSpaces, GetSegments, Consume*, Peek):
   ```go
   spaces := client.GetSpaces(ctx, storeID)
   for spaces.MoveNext() {
       space, _ := spaces.Current()
       // process space
   }
   ```

2. **Factory Pattern**: Storage, Transport, and Client creation via factories:
   - `StoreFactory.NewStore(ctx, storeID)`
   - `BidiStreamProvider` creates providers for clients
   - Enables swappable implementations

3. **Context-Driven Configuration**: Heavy use of `context.Context` for cancellation, deadlines, and metadata:
   - `WithChannelID()` attaches channel identifiers to context
   - All operations accept `ctx` as first parameter

4. **Polymorphic Serialization**: Use `Routeable` + `Polymorphic` for messages that need runtime type discrimination across network boundaries

5. **Binary Codec + Compression**: All persistence uses `internal/codec/`:
   - Binary serialization for efficiency (vs JSON where possible)
   - Snappy compression standard (`EncodeEntrySnappy()`)
   - Constants: `DATA`, `INVENTORY`, `TRANSACTION`, `SPACES`, `SEGMENTS` prefix all storage keys

6. **Bidirectional Streaming**: All transports abstract to `BidiStream` interface:
   - Must support full-duplex simultaneous read/write
   - `Muxer` multiplexes independent channels over single stream

## 🚦 Dependencies & Integration Points

**Key External Packages** (from `go.mod`):
- `fgrzl/enumerators`: Iterator abstraction used throughout
- `fgrzl/json`: Polymorphic serialization framework
- `fgrzl/lexkey`, `fgrzl/mux`: Custom utilities
- `cockroachdb/pebble/v2`: Local embedded storage
- `Azure/azure-sdk-for-go`: Cloud storage backend
- `google/uuid`: Identifiers for stores/channels
- `golang/snappy`: Compression codec
- `stretchr/testify`: Testing assertions

**Storage Backend Selection**:
- Local: Use `pebblekit.Factory` 
- Cloud: Use `azurekit.Factory` with Azure credentials
- Both implement identical `Store` interface

## 📝 Common Task Patterns

- **Adding a new operation**: Define in `pkg/api/` first (request/response), implement in `pkg/storage/` backends, expose via `pkg/client/`
- **Adding transport**: Implement `BidiStream` interface, create Muxer, add Provider
- **Storage optimization**: Look at `internal/codec/` for serialization, `internal/cache/` for caching patterns
- **Testing integrations**: Use `test/setup_data.go` for fixtures, follow integration_test.go patterns
