// Package api defines the core message types, interfaces, and structures
// used for communication between streamkit clients and servers.
//
// This package contains protocol definitions for streaming operations,
// including consumption, production, subscription, and status messages.
// It also defines the bidirectional stream interface and error types
// used throughout the streamkit system.
package api

import (
	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/lexkey"
	"github.com/google/uuid"
)

func init() {
	polymorphic.Register(func() *Consume { return &Consume{} })
	polymorphic.Register(func() *ConsumeSegment { return &ConsumeSegment{} })
	polymorphic.Register(func() *ConsumeSpace { return &ConsumeSpace{} })
	polymorphic.Register(func() *GetSpaces { return &GetSpaces{} })
	polymorphic.Register(func() *GetSegments { return &GetSegments{} })
	polymorphic.Register(func() *GetStatus { return &GetStatus{} })
	polymorphic.Register(func() *Peek { return &Peek{} })
	polymorphic.Register(func() *Produce { return &Produce{} })
	polymorphic.Register(func() *SegmentStatus { return &SegmentStatus{} })
	polymorphic.Register(func() *SubscribeToSegmentStatus { return &SubscribeToSegmentStatus{} })
}

// ─── Notification & Subscription ───────────────────────────────────────────────

// SubscribeToSegmentStatus represents a subscription request for segment status updates.
type SubscribeToSegmentStatus struct {
	Space   string `json:"space"`
	Segment string `json:"segment"`
}

// GetDiscriminator returns the unique message type identifier for SubscribeToSegmentStatus.
func (m *SubscribeToSegmentStatus) GetDiscriminator() string {
	return "streamkit://api/v1/subscribe_to_segment_status"
}

// SegmentStatus contains metadata about a segment's current state,
// including sequence numbers and timestamps of the first and last entries.
type SegmentStatus struct {
	Space          string `json:"space"`
	Segment        string `json:"segment"`
	FirstSequence  uint64 `json:"first_sequence"`
	FirstTimestamp int64  `json:"first_timestamp"`
	LastSequence   uint64 `json:"last_sequence"`
	LastTimestamp  int64  `json:"last_timestamp"`
}

// GetDiscriminator returns the unique message type identifier for SegmentStatus.
func (m *SegmentStatus) GetDiscriminator() string {
	return "streamkit://api/v1/segment_status"
}

// ─── API Messages ──────────────────────────────────────────────────────────────

// Peek represents a request to read the latest entry from a segment without consuming it.
type Peek struct {
	Space   string `json:"space"`
	Segment string `json:"segment"`
}

// GetDiscriminator returns the unique message type identifier for Peek.
func (m *Peek) GetDiscriminator() string {
	return "streamkit://api/v1/peek"
}

// Produce represents a request to write records to a specific segment.
type Produce struct {
	Space   string `json:"space"`
	Segment string `json:"segment"`
}

// GetDiscriminator returns the unique message type identifier for Produce.
func (m *Produce) GetDiscriminator() string {
	return "streamkit://api/v1/produce"
}

// Record represents a data record to be written to a segment.
type Record struct {
	Sequence uint64            `json:"sequence"`
	Payload  []byte            `json:"payload"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Entry represents a stored data entry read from a segment, including
// transaction information and positioning metadata.
type Entry struct {
	Sequence  uint64            `json:"sequence"`
	Timestamp int64             `json:"timestamp,omitempty"`
	TRX       TRX               `json:"trx"`
	Payload   []byte            `json:"payload"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Space     string            `json:"space"`
	Segment   string            `json:"segment"`
}

// GetSpaceOffset returns the lexicographic key for this entry's position within its space.
func (e *Entry) GetSpaceOffset() lexkey.LexKey {
	return lexkey.Encode(DATA, SPACES, e.Space, e.Timestamp, e.Segment, e.Sequence)
}

// GetSegmentOffset returns the lexicographic key for this entry's position within its segment.
func (e *Entry) GetSegmentOffset() lexkey.LexKey {
	return lexkey.Encode(DATA, SEGMENTS, e.Space, e.Segment, e.Sequence)
}

// Consume represents a request to read entries from multiple spaces with optional filters.
type Consume struct {
	MinTimestamp int64                    `json:"min_timestamp,omitempty"`
	MaxTimestamp int64                    `json:"max_timestamp,omitempty"`
	Offsets      map[string]lexkey.LexKey `json:"offsets,omitempty"`
}

// GetDiscriminator returns the unique message type identifier for Consume.
func (m *Consume) GetDiscriminator() string {
	return "streamkit://api/v1/consume"
}

// ConsumeSpace represents a request to read entries from all segments within a space.
type ConsumeSpace struct {
	Space        string        `json:"space"`
	MinTimestamp int64         `json:"min_timestamp,omitempty"`
	MaxTimestamp int64         `json:"max_timestamp,omitempty"`
	Offset       lexkey.LexKey `json:"offset,omitempty"`
}

// GetDiscriminator returns the unique message type identifier for ConsumeSpace.
func (m *ConsumeSpace) GetDiscriminator() string {
	return "streamkit://api/v1/consume_space"
}

// ConsumeSegment represents a request to read entries from a specific segment with optional filters.
type ConsumeSegment struct {
	Space        string `json:"space"`
	Segment      string `json:"segment"`
	MinSequence  uint64 `json:"min_sequence,omitempty"`
	MinTimestamp int64  `json:"min_timestamp,omitempty"`
	MaxSequence  uint64 `json:"max_sequence,omitempty"`
	MaxTimestamp int64  `json:"max_timestamp,omitempty"`
}

// GetDiscriminator returns the unique message type identifier for ConsumeSegment.
func (m *ConsumeSegment) GetDiscriminator() string {
	return "streamkit://api/v1/consume_segment"
}

// GetSpaces represents a request to list all available spaces in a store.
type GetSpaces struct{}

// GetDiscriminator returns the unique message type identifier for GetSpaces.
func (m *GetSpaces) GetDiscriminator() string {
	return "streamkit://api/v1/get_spaces"
}

// GetSegments represents a request to list all segments within a specific space.
type GetSegments struct {
	Space string `json:"space"`
}

// GetDiscriminator returns the unique message type identifier for GetSegments.
func (m *GetSegments) GetDiscriminator() string {
	return "streamkit://api/v1/get_segments"
}

// GetStatus represents a request for general store status information.
type GetStatus struct{}

// GetDiscriminator returns the unique message type identifier for GetStatus.
func (m *GetStatus) GetDiscriminator() string {
	return "streamkit://api/v1/get_status"
}

// TRX represents transaction metadata associated with an entry.
type TRX struct {
	ID     uuid.UUID `json:"id"`
	Node   uuid.UUID `json:"node,omitempty"`
	Number uint64    `json:"number"`
}
