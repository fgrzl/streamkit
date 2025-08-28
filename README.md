
[![CI](https://github.com/fgrzl/streamkit/actions/workflows/ci.yaml/badge.svg)](https://github.com/fgrzl/streamkit/actions/workflows/ci.yaml)
[![CI](https://github.com/fgrzl/streamkit/actions/workflows/pre-release.yaml/badge.svg)](https://github.com/fgrzl/streamkit/actions/workflows/pre-release.yaml)
[![Dependabot Updates](https://github.com/fgrzl/streamkit/actions/workflows/dependabot/dependabot-updates/badge.svg)](https://github.com/fgrzl/streamkit/actions/workflows/dependabot/dependabot-updates)


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
