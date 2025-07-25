// Package storage defines interfaces and implementations for data persistence
// in the streamkit streaming platform.
//
// This package provides abstractions for storing and retrieving stream data,
// including support for different storage backends like PebbleDB and Azure Tables.
// The Store interface defines the core operations for managing spaces, segments,
// and their associated data entries.
package storage

import (
	"context"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/google/uuid"
)

// StoreFactory defines how to create new storage instances by name.
type StoreFactory interface {
	// NewStore creates a new Store instance for the given store identifier.
	NewStore(ctx context.Context, storeID uuid.UUID) (Store, error)
}

// Store provides the core interface for data persistence operations
// within the streamkit streaming platform.
type Store interface {
	// GetSpaces returns an enumerator of all available space names.
	GetSpaces(ctx context.Context) enumerators.Enumerator[string]
	
	// ConsumeSpace reads entries from all segments within a space, ordered by timestamp.
	ConsumeSpace(ctx context.Context, args *api.ConsumeSpace) enumerators.Enumerator[*api.Entry]
	
	// GetSegments returns an enumerator of all segment names within the specified space.
	GetSegments(ctx context.Context, space string) enumerators.Enumerator[string]
	
	// ConsumeSegment reads entries from a specific segment in sequence order.
	ConsumeSegment(ctx context.Context, args *api.ConsumeSegment) enumerators.Enumerator[*api.Entry]
	
	// Peek returns the latest entry from the specified segment without consuming it.
	Peek(ctx context.Context, space, segment string) (*api.Entry, error)
	
	// Produce writes a stream of records to the specified segment and returns status updates.
	Produce(ctx context.Context, args *api.Produce, entries enumerators.Enumerator[*api.Record]) enumerators.Enumerator[*api.SegmentStatus]
	
	// Close releases all resources associated with the store.
	Close()
}
