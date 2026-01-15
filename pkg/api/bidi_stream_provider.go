package api

import (
	"context"

	"github.com/google/uuid"
)

// StreamSubscriptionHandler defines a function type for handling streaming subscriptions.
type StreamSubscriptionHandler func(Routeable, BidiStream)

// BidiStreamProvider creates bidirectional streams for communication with remote handlers.
type BidiStreamProvider interface {
	// CallStream initiates a bidirectional stream to a remote handler.
	// It returns a BidiStream that allows peeking, producing, and consuming messages.
	// This is distinct from domain-level data streams.
	CallStream(ctx context.Context, storeID uuid.UUID, routeable Routeable) (BidiStream, error)
}
