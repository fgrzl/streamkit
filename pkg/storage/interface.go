package storage

import (
	"context"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
)

// StoreFactory defines how to create new storage instances by name.
type StoreFactory interface {
	NewStore(ctx context.Context, storeID uuid.UUID) (Store, error)
}

type Store interface {
	GetSpaces(ctx context.Context) enumerators.Enumerator[string]
	ConsumeSpace(ctx context.Context, args *api.ConsumeSpace) enumerators.Enumerator[*api.Entry]
	GetSegments(ctx context.Context, space string) enumerators.Enumerator[string]
	ConsumeSegment(ctx context.Context, args *api.ConsumeSegment) enumerators.Enumerator[*api.Entry]
	Peek(ctx context.Context, space, segment string) (*api.Entry, error)
	Produce(ctx context.Context, args *api.Produce, entries enumerators.Enumerator[*api.Record]) enumerators.Enumerator[*api.SegmentStatus]
	Close()
}
