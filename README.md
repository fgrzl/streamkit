[![ci](https://github.com/fgrzl/streamkit/actions/workflows/ci.yml/badge.svg)](https://github.com/fgrzl/streamkit/actions/workflows/ci.yml)
[![Dependabot Updates](https://github.com/fgrzl/streamkit/actions/workflows/dependabot/dependabot-updates/badge.svg)](https://github.com/fgrzl/streamkit/actions/workflows/dependabot/dependabot-updates)

# Streamkit  

#### Organizing Streams: Spaces and Segments  
Streams use a hierarchical model—**Spaces** and **Segments**—to efficiently manage and consume high-throughput event data.

### Store
A **Store** provides the physical separation at a storge level. 

### Spaces  
A **Space** is a top-level logical container for related streams.

- Groups streams by application, data type, or service  
- Enables broad categorization and easier management  
- Consumers can subscribe to an entire Space for interleaved events from all Segments  

### Segments  
**Segments** are independent, ordered sub-streams within a Space.

- Maintain strict event order  
- Support parallel consumption for scalability  
- Identified uniquely within their Space  

### How It Works  

- **Producing**: Data is written to specific Segments in a Space within a Store
- **Consuming**:  
  - Subscribe to a Space for all Segments (interleaved)  
  - Subscribe to a Segment for strict ordering  
- **Peeking**: Read the latest entry in a Segment without consuming it  
- **Offsets & Transactions**:  
  - Offsets track consumer progress  
  - Transactions ensure consistent writes  
