package api

import (
	"context"

	"github.com/google/uuid"
)

// StreamSubscriptionHandler defines a function type for handling streaming subscriptions.
type StreamSubscriptionHandler func(Routeable, BidiStream)

// ReconnectListener receives notifications when a provider endpoint reconnects.
type ReconnectListener interface {
	// OnReconnected is called when the provider successfully reconnects to a backend.
	// This allows clients to replay subscriptions or refresh state.
	OnReconnected(ctx context.Context, storeID uuid.UUID) error
}

// BidiStreamProvider creates bidirectional streams for communication with remote handlers.
type BidiStreamProvider interface {
	// CallStream initiates a bidirectional stream to a remote handler.
	// It returns a BidiStream that allows peeking, producing, and consuming messages.
	// This is distinct from domain-level data streams.
	CallStream(ctx context.Context, storeID uuid.UUID, routeable Routeable) (BidiStream, error)

	// RegisterReconnectListener registers a listener to be notified when the provider reconnects.
	// Multiple listeners can be registered and will all be called sequentially.
	RegisterReconnectListener(listener ReconnectListener)

	// UnregisterReconnectListener removes a previously registered reconnect listener.
	UnregisterReconnectListener(listener ReconnectListener)
}
