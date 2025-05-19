package storage

import (
	"context"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit/pkg/api"
)

type Store interface {
	GetSpaces(ctx context.Context) enumerators.Enumerator[string]
	ConsumeSpace(ctx context.Context, args *api.ConsumeSpace) enumerators.Enumerator[*api.Entry]
	GetSegments(ctx context.Context, space string) enumerators.Enumerator[string]
	ConsumeSegment(ctx context.Context, args *api.ConsumeSegment) enumerators.Enumerator[*api.Entry]
	Peek(ctx context.Context, space, segment string) (*api.Entry, error)
	Produce(ctx context.Context, args *api.Produce, entries enumerators.Enumerator[*api.Record]) enumerators.Enumerator[*api.SegmentStatus]
	Close() error
}

type Service struct {
	storage Store
}

func (s *Service) Consume(ctx context.Context, args *api.Consume) enumerators.Enumerator[*api.Entry] {
	spaces := make([]enumerators.Enumerator[*api.Entry], 0, len(args.Offsets))
	for space, offset := range args.Offsets {
		spaces = append(spaces, s.storage.ConsumeSpace(ctx, &api.ConsumeSpace{
			Space:        space,
			MinTimestamp: args.MinTimestamp,
			MaxTimestamp: args.MaxTimestamp,
			Offset:       offset,
		}))
	}
	return enumerators.Interleave(spaces, func(e *api.Entry) int64 { return e.Timestamp })
}
