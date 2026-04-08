[![CI](https://github.com/fgrzl/streamkit/actions/workflows/ci.yml/badge.svg)](https://github.com/fgrzl/streamkit/actions/workflows/ci.yml)
[![CI](https://github.com/fgrzl/streamkit/actions/workflows/publish.yml/badge.svg)](https://github.com/fgrzl/streamkit/actions/workflows/publish.yml)

# Streamkit

> **Streamkit** is a high-throughput, hierarchical event streaming platform designed for scalable, organized, and reliable data flows.

## 🧩 Core Concepts

A **Store** provides physical separation at the storage level, acting as the root for all spaces and segments.

A **Space** is a top-level logical container for related streams.

- Group streams by application, data type, or service
- Enable broad categorization and easier management
- Consumers can subscribe to an entire Space to receive interleaved events from all Segments

**Segments** are independent, ordered sub-streams within a Space.

- Maintain strict event order
- Support parallel consumption for scalability
- Are uniquely identified within their Space

## ⚙️ How It Works

- **Producing:** Data is written to specific Segments in a Space within a Store
- **Consuming:**
  - Subscribe to a Space for all Segments (interleaved)
  - Subscribe to a Segment for strict ordering
- **Peeking:** Read the latest entry in a Segment without consuming it
- **Offsets & Transactions:**
  - Offsets track consumer progress
  - Transactions ensure consistent writes

## 🚦 Getting Started

```go
import (
    "context"
    "github.com/fgrzl/streamkit"
    "github.com/fgrzl/streamkit/pkg/transport/prockit"
    "github.com/google/uuid"
)

func main() {
    provider := prockit.NewBidiStreamProvider()
    client := streamkit.NewClient(provider)
    ctx := context.Background()
    storeID := uuid.New() // Replace with your store UUID

    // Example: List spaces
    spaces := client.GetSpaces(ctx, storeID)
    for spaces.MoveNext() {
        space, _ := spaces.Current()
        println("Space:", space)
    }
}
```

## 📋 Known Limitations (Alpha)

### Client Resilience

The client includes production-grade resilience features but has documented limitations:

1. **Lock Management**: Per-segment produce locks accumulate over time. Not an issue for typical workloads (<10K segments) but may impact scenarios with millions of unique segments over time.

2. **Deduplication**: While resilience enumerators resume from last consumed position, applications should implement sequence-based deduplication for exactly-once semantics in critical workflows.

3. **Subscription Lifecycle**: Retryable subscription failures stay in the reconnect loop, but permanent errors stop the subscription and remove it from the client registry. `SubscribeToSpace()` is the wildcard form of `SubscribeToSegmentStatus` (`Segment: "*"`) and reconnects receive a latest-state snapshot before live updates resume.

**Recommendations for Alpha Testing:**

- ✅ Treat subscription handlers as latest-state processors: reconnects deliver a fresh snapshot, then continue live updates
- ✅ Implement application-level deduplication using `Entry.Sequence` numbers
- ✅ Monitor segment churn if creating more than 10K unique segments
- ✅ Handle permanent subscription failures explicitly in application code and logs

For detailed changes and future roadmap, see [CHANGELOG.md](CHANGELOG.md).
