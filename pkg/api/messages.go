package api

import (
	"encoding/json"
	"fmt"

	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/lexkey"
	"github.com/google/uuid"
)

func init() {
	polymorphic.Register(func() *ACK { return &ACK{} })
	polymorphic.Register(func() *Commit { return &Commit{} })
	polymorphic.Register(func() *ConfirmSegmentOffset { return &ConfirmSegmentOffset{} })
	polymorphic.Register(func() *ConfirmSpaceOffset { return &ConfirmSpaceOffset{} })
	polymorphic.Register(func() *Consume { return &Consume{} })
	polymorphic.Register(func() *ConsumeSegment { return &ConsumeSegment{} })
	polymorphic.Register(func() *ConsumeSpace { return &ConsumeSpace{} })
	polymorphic.Register(func() *EnumerateSegment { return &EnumerateSegment{} })
	polymorphic.Register(func() *EnumerateSpace { return &EnumerateSpace{} })
	polymorphic.Register(func() *GetSegments { return &GetSegments{} })
	polymorphic.Register(func() *GetSpaces { return &GetSpaces{} })
	polymorphic.Register(func() *GetStatus { return &GetStatus{} })
	polymorphic.Register(func() *NACK { return &NACK{} })
	polymorphic.Register(func() *NodeHeartbeat { return &NodeHeartbeat{} })
	polymorphic.Register(func() *NodeShutdown { return &NodeShutdown{} })
	polymorphic.Register(func() *Peek { return &Peek{} })
	polymorphic.Register(func() *Produce { return &Produce{} })
	polymorphic.Register(func() *Reconcile { return &Reconcile{} })
	polymorphic.Register(func() *Rollback { return &Rollback{} })
	polymorphic.Register(func() *SegmentStatus { return &SegmentStatus{} })
	polymorphic.Register(func() *Synchronize { return &Synchronize{} })
	polymorphic.Register(func() *Transaction { return &Transaction{} })
	polymorphic.Register(func() *TRX { return &TRX{} })
}

type SubscribeToSpaceNotification struct {
	Space string `json:"space"`
}

func (s *SubscribeToSpaceNotification) GetDiscriminator() string {
	return "streamkit://subscriptions/space"
}

type SegmentStatus struct {
	Space          string `json:"space"`
	Segment        string `json:"segment"`
	FirstSequence  uint64 `json:"first_sequence"`
	FirstTimestamp int64  `json:"first_timestamp"`
	LastSequence   uint64 `json:"last_sequence"`
	LastTimestamp  int64  `json:"last_timestamp"`
}

func (s *SegmentStatus) GetDiscriminator() string {
	return fmt.Sprintf("%T", s)
}

func (s *SegmentStatus) GetRoute() string {
	return "segment_status"
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

type GetStatus struct{}

func (g *GetStatus) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (g *GetStatus) GetRoute() string {
	return "get_cluster_status"
}

type ClusterStatus struct {
	NodeCount int `json:"node_count"`
}

type Peek struct {
	Space   string `json:"space"`
	Segment string `json:"segment"`
}

func (g *Peek) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (g *Peek) GetRoute() string {
	return "peek"
}

type Produce struct {
	Space   string `json:"space"`
	Segment string `json:"segment"`
}

func (g *Produce) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (g *Produce) GetRoute() string {
	return "produce"
}

type Consume struct {
	MinTimestamp int64                    `json:"min_timestamp,omitempty"`
	MaxTimestamp int64                    `json:"max_timestamp,omitempty"`
	Offsets      map[string]lexkey.LexKey `json:"offsets,omitempty"`
}

func (g *Consume) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (g *Consume) GetRoute() string {
	return "consume"
}

type ConsumeSpace struct {
	Space        string        `json:"space"`
	MinTimestamp int64         `json:"min_timestamp,omitempty"`
	MaxTimestamp int64         `json:"max_timestamp,omitempty"`
	Offset       lexkey.LexKey `json:"offset,omitempty"`
}

func (g *ConsumeSpace) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (g *ConsumeSpace) GetRoute() string {
	return "consume_space"
}

type ConsumeSegment struct {
	Space   string `json:"space"`
	Segment string `json:"segment"`

	// The minimum sequence number to consume.
	MinSequence  uint64 `json:"min_sequence,omitempty"`
	MinTimestamp int64  `json:"min_timestamp,omitempty"`
	MaxSequence  uint64 `json:"max_sequence,omitempty"`
	MaxTimestamp int64  `json:"max_timestamp,omitempty"`
}

func (g *ConsumeSegment) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (g *ConsumeSegment) GetRoute() string {
	return "consume_segment"
}

//
// Data Management
//

type GetSpaces struct{}

func (g *GetSpaces) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (g *GetSpaces) GetRoute() string {
	return "get_spaces"
}

type GetSegments struct {
	Space string `json:"space"`
}

func (g *GetSegments) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (g *GetSegments) GetRoute() string {
	return "get_segments"
}

type EnumerateSpace struct {
	Space        string        `json:"space"`
	MinTimestamp int64         `json:"min_timestamp,omitempty"`
	MaxTimestamp int64         `json:"max_timestamp,omitempty"`
	Offset       lexkey.LexKey `json:"offset,omitempty"`
}

func (g *EnumerateSpace) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (g *EnumerateSpace) GetRoute() string {
	return "enumerate_space"
}

type EnumerateSegment struct {
	Space   string `json:"space"`
	Segment string `json:"segment"`

	// The minimum sequence number to consume.
	MinSequence  uint64 `json:"min_sequence,omitempty"`
	MinTimestamp int64  `json:"min_timestamp,omitempty"`
	MaxSequence  uint64 `json:"max_sequence,omitempty"`
	MaxTimestamp int64  `json:"max_timestamp,omitempty"`
}

func (g *EnumerateSegment) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (g *EnumerateSegment) GetRoute() string {
	return "enumerate_segment"
}

type ConfirmSpaceOffset struct {
	ID     uuid.UUID     `json:"id"`
	Node   uuid.UUID     `json:"node"`
	Space  string        `json:"space"`
	Offset lexkey.LexKey `json:"offset"`
}

func (g *ConfirmSpaceOffset) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (c *ConfirmSpaceOffset) GetRoute() string {
	return "check_space_offset"
}

func (c *ConfirmSpaceOffset) ToACK(node uuid.UUID) *ACK {
	return &ACK{
		ID:   c.ID,
		Node: node,
	}
}

func (c *ConfirmSpaceOffset) ToNACK(node uuid.UUID) *NACK {
	return &NACK{
		ID:   c.ID,
		Node: node,
	}
}

type ConfirmSegmentOffset struct {
	ID      uuid.UUID     `json:"id"`
	Node    uuid.UUID     `json:"node"`
	Space   string        `json:"space"`
	Segment string        `json:"segment"`
	Offset  lexkey.LexKey `json:"offset"`
}

func (g *ConfirmSegmentOffset) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (c *ConfirmSegmentOffset) GetRoute() string {
	return "check_segment_offset"
}

func (c *ConfirmSegmentOffset) ToACK(node uuid.UUID) *ACK {
	return &ACK{
		ID:   c.ID,
		Node: node,
	}
}

func (c *ConfirmSegmentOffset) ToNACK(node uuid.UUID) *NACK {
	return &NACK{
		ID:   c.ID,
		Node: node,
	}
}

//
// Transaction Management
//

const (
	UNCOMMITTED = "uncommitted"
	COMMITTED   = "committed"
	FINALIZED   = "finalized"
)

type Transaction struct {
	TRX           TRX      `json:"trx"`
	Space         string   `json:"space"`
	Segment       string   `json:"segment"`
	FirstSequence uint64   `json:"first_sequence"`
	LastSequence  uint64   `json:"last_sequence"`
	Entries       []*Entry `json:"entries"`
	Timestamp     int64    `json:"timestamp"`
}

func (g *Transaction) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (a *Transaction) GetRoute() string {
	return fmt.Sprintf("%T", a)
}

func (t *Transaction) MarshalJSON() ([]byte, error) {

	raw, err := EncodeTransaction(t)
	if err != nil {
		return nil, err
	}

	wrapper := struct {
		D []byte `json:"d"`
	}{D: raw}

	return json.Marshal(wrapper)
}

func (t *Transaction) UnmarshalJSON(data []byte) error {
	wrapper := struct {
		D []byte `json:"d"`
	}{}

	if err := json.Unmarshal(data, &wrapper); err != nil {
		return fmt.Errorf("failed to unmarshal wrapper: %w", err)
	}

	if len(wrapper.D) == 0 {
		return fmt.Errorf("compressed data is empty")
	}

	return DecodeTransaction(wrapper.D, t)
}

type TRX struct {
	ID     uuid.UUID `json:"id"`
	Node   uuid.UUID `json:"node,omitempty"`
	Number uint64    `json:"number"`
}

func (g *TRX) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (a *TRX) GetRoute() string {
	return fmt.Sprintf("%T.%v", a, a.ID)
}

func (a *TRX) ToACK(node uuid.UUID) *ACK {
	return &ACK{
		ID:   a.ID,
		Node: node,
	}
}

func (a *TRX) ToNACK(node uuid.UUID) *NACK {
	return &NACK{
		ID:   a.ID,
		Node: node,
	}
}

type Commit struct {
	TRX     TRX    `json:"trx"`
	Space   string `json:"space"`
	Segment string `json:"segment"`
}

func (g *Commit) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (a *Commit) GetRoute() string {
	return "trx.commit"
}

type Reconcile struct {
	TRX     TRX    `json:"trx"`
	Space   string `json:"space"`
	Segment string `json:"segment"`
}

func (g *Reconcile) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (a *Reconcile) GetRoute() string {
	return "trx.reconcile"
}

type Rollback struct {
	TRX     TRX    `json:"trx"`
	Space   string `json:"space"`
	Segment string `json:"segment"`
}

func (g *Rollback) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (a *Rollback) GetRoute() string {
	return "trx.rollback"
}

//
// Node Management
//

type Synchronize struct {
	OffsetsBySpace map[string]lexkey.LexKey `json:"offsets_by_space"`
}

func (g *Synchronize) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (a *Synchronize) GetRoute() string {
	return "node.synchronize"
}

// NodeHeartbeat represents a node failure event
type NodeHeartbeat struct {
	Node  uuid.UUID           `json:"node"`
	Nodes map[uuid.UUID]int64 `json:"nodes"`
}

func (g *NodeHeartbeat) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (h *NodeHeartbeat) GetRoute() string {
	return "node.heartbeat"
}

// NodeShutdown notifies that a node has gone down
type NodeShutdown struct {
	Node uuid.UUID `json:"node"`
}

func (g *NodeShutdown) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (n *NodeShutdown) GetRoute() string {
	return "node.shutdown"
}

//
// ACK and NACK
//

type ACK struct {
	ID   uuid.UUID `json:"id"`
	Node uuid.UUID `json:"node"`
}

func (g *ACK) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (a *ACK) GetRoute() string {
	return GetReplyRoute(a.ID)
}

type NACK struct {
	ID   uuid.UUID `json:"id"`
	Node uuid.UUID `json:"node"`
}

func (g *NACK) GetDiscriminator() string {
	return fmt.Sprintf("%T", g)
}

func (a *NACK) GetRoute() string {
	return GetReplyRoute(a.ID)
}

func GetReplyRoute(messageID uuid.UUID) string {
	// creates a unique reply route from the messageID
	return "reply." + messageID.String()
}
