package node

import (
	"context"

	"github.com/google/uuid"
)

// channelKey is an unexported type for context keys in this package.
type channelKey struct{}

// WithChannelID returns a new context with the given channelID attached.
func WithChannelID(ctx context.Context, id uuid.UUID) context.Context {
	return context.WithValue(ctx, channelKey{}, id)
}

// ChannelIDFromContext retrieves the channelID from the context, if present.
func ChannelIDFromContext(ctx context.Context) (uuid.UUID, bool) {
	id, ok := ctx.Value(channelKey{}).(uuid.UUID)
	return id, ok
}
