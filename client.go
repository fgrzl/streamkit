package streamkit

import (
	"context"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit/pkg/api"
)

type Entry = api.Entry
type Record = api.Record
type SegmentStatus = api.SegmentStatus
type ConsumeSpace = api.ConsumeSpace
type Consume = api.Consume
type ConsumeSegment = api.ConsumeSegment
type Produce = api.Produce
type GetSpaces = api.GetSpaces
type GetSegments = api.GetSegments
type Peek = api.Peek
type GetStatus = api.GetStatus
type ClusterStatus = api.ClusterStatus

type Client interface {

	// Get Node Count
	GetClusterStatus(ctx context.Context) (*ClusterStatus, error)

	// Get all the spaces
	GetSpaces(ctx context.Context) enumerators.Enumerator[string]

	// Get all segments in a space.
	GetSegments(ctx context.Context, space string) enumerators.Enumerator[string]

	// Get the last entry in a stream.
	Peek(ctx context.Context, space, segment string) (*Entry, error)

	// Consume the space. This will interleave all of the streams in the space.
	Consume(ctx context.Context, args *Consume) enumerators.Enumerator[*Entry]

	// Consume the space. This will interleave all of the streams in the space.
	ConsumeSpace(ctx context.Context, args *ConsumeSpace) enumerators.Enumerator[*Entry]

	// Consume a segment.
	ConsumeSegment(ctx context.Context, args *ConsumeSegment) enumerators.Enumerator[*Entry]

	// Produce stream entries.
	Produce(ctx context.Context, space, segment string, entries enumerators.Enumerator[*Record]) enumerators.Enumerator[*SegmentStatus]

	// Publish one off events.
	Publish(ctx context.Context, space, segment string, payload []byte, metadata map[string]string) error

	// Subscribe to a space.
	SubcribeToSpace(ctx context.Context, space string, handler func(*SegmentStatus)) (api.Subscription, error)

	// Subscribe to a segment.
	SubcribeToSegment(ctx context.Context, space, segment string, handler func(*SegmentStatus)) (api.Subscription, error)
}
