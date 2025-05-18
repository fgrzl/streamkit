package api

import "context"

type StreamSubscriptionHandler func(Routeable, BidiStream)

type Bus interface {
	// CallStream initiates a bidirectional stream to a remote handler.
	// It returns a BidiStream that allows peeking, producing, and consuming messages.
	// This is distinct from domain-level data streams.
	CallStream(ctx context.Context, msg Routeable) (BidiStream, error)
}
