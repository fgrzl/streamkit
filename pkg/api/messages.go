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

type SubscribeToSegmentStatus struct {
	Space   string `json:"space"`
	Segment string `json:"segment"`
}

func (m *SubscribeToSegmentStatus) GetDiscriminator() string {
	return "streamkit://api/v1/subscribe_to_segment_status"
}

type SegmentStatus struct {
	Space          string `json:"space"`
	Segment        string `json:"segment"`
	FirstSequence  uint64 `json:"first_sequence"`
	FirstTimestamp int64  `json:"first_timestamp"`
	LastSequence   uint64 `json:"last_sequence"`
	LastTimestamp  int64  `json:"last_timestamp"`
}

func (m *SegmentStatus) GetDiscriminator() string {
	return "streamkit://api/v1/segment_status"
}

// ─── API Messages ──────────────────────────────────────────────────────────────

type Peek struct {
	Space   string `json:"space"`
	Segment string `json:"segment"`
}

func (m *Peek) GetDiscriminator() string {
	return "streamkit://api/v1/peek"
}

type Produce struct {
	Space   string `json:"space"`
	Segment string `json:"segment"`
}

func (m *Produce) GetDiscriminator() string {
	return "streamkit://api/v1/produce"
}

type Record struct {
	Sequence uint64            `json:"sequence"`
	Payload  []byte            `json:"payload"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type Entry struct {
	Sequence  uint64            `json:"sequence"`
	Timestamp int64             `json:"timestamp,omitempty"`
	TRX       TRX               `json:"trx"`
	Payload   []byte            `json:"payload"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Space     string            `json:"space"`
	Segment   string            `json:"segment"`
}

func (e *Entry) GetSpaceOffset() lexkey.LexKey {
	return lexkey.Encode(DATA, SPACES, e.Space, e.Timestamp, e.Segment, e.Sequence)
}

func (e *Entry) GetSegmentOffset() lexkey.LexKey {
	return lexkey.Encode(DATA, SEGMENTS, e.Space, e.Segment, e.Sequence)
}

type Consume struct {
	MinTimestamp int64                    `json:"min_timestamp,omitempty"`
	MaxTimestamp int64                    `json:"max_timestamp,omitempty"`
	Offsets      map[string]lexkey.LexKey `json:"offsets,omitempty"`
}

func (m *Consume) GetDiscriminator() string {
	return "streamkit://api/v1/consume"
}

type ConsumeSpace struct {
	Space        string        `json:"space"`
	MinTimestamp int64         `json:"min_timestamp,omitempty"`
	MaxTimestamp int64         `json:"max_timestamp,omitempty"`
	Offset       lexkey.LexKey `json:"offset,omitempty"`
}

func (m *ConsumeSpace) GetDiscriminator() string {
	return "streamkit://api/v1/consume_space"
}

type ConsumeSegment struct {
	Space        string `json:"space"`
	Segment      string `json:"segment"`
	MinSequence  uint64 `json:"min_sequence,omitempty"`
	MinTimestamp int64  `json:"min_timestamp,omitempty"`
	MaxSequence  uint64 `json:"max_sequence,omitempty"`
	MaxTimestamp int64  `json:"max_timestamp,omitempty"`
}

func (m *ConsumeSegment) GetDiscriminator() string {
	return "streamkit://api/v1/consume_segment"
}

type GetSpaces struct{}

func (m *GetSpaces) GetDiscriminator() string {
	return "streamkit://api/v1/get_spaces"
}

type GetSegments struct {
	Space string `json:"space"`
}

func (m *GetSegments) GetDiscriminator() string {
	return "streamkit://api/v1/get_segments"
}

type GetStatus struct{}

func (m *GetStatus) GetDiscriminator() string {
	return "streamkit://api/v1/get_status"
}

type TRX struct {
	ID     uuid.UUID `json:"id"`
	Node   uuid.UUID `json:"node,omitempty"`
	Number uint64    `json:"number"`
}
