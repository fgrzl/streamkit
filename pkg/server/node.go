// Package server provides the core server-side request handling and coordination logic
// for the streamkit streaming platform.
//
// This package implements the Node interface which processes incoming
// requests and coordinates with storage backends to fulfill streaming
// operations. It also manages node lifecycle and provides notification
// capabilities for real-time updates.
package server

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/messaging"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/storage"
	"github.com/google/uuid"
)

// Node represents a request handler that processes streaming operations
// and coordinates with storage backends.
type Node interface {
	// Handle processes incoming bidirectional stream requests.
	Handle(context.Context, api.BidiStream)
	// Close releases all resources associated with the node.
	Close()
}

// NewNode creates a new Node instance with the specified store and optional message bus factory.
func NewNode(storeID uuid.UUID, store storage.Store, busFactory messaging.MessageBusFactory) Node {
	return &defaultNode{
		storeID:    storeID,
		store:      store,
		busFactory: busFactory,
	}
}

type defaultNode struct {
	storeID    uuid.UUID
	store      storage.Store
	busFactory messaging.MessageBusFactory
}

func (n *defaultNode) Close() {
	n.store.Close()
}

func (n *defaultNode) Handle(ctx context.Context, bidi api.BidiStream) {

	defer func() {
		if r := recover(); r != nil {
			bidi.Close(fmt.Errorf("panic: %v", r))
		}
	}()

	envelope := &polymorphic.Envelope{}
	if err := bidi.Decode(envelope); err != nil {
		bidi.Close(err)
		return
	}

	switch args := envelope.Content.(type) {
	case *api.Consume:
		n.handleConsume(ctx, args, bidi)
	case *api.ConsumeSegment:
		n.handleConsumeSegment(ctx, args, bidi)
	case *api.ConsumeSpace:
		n.handleConsumeSpace(ctx, args, bidi)
	case *api.GetSegments:
		n.handleGetSegments(ctx, args, bidi)
	case *api.GetSpaces:
		n.handleGetSpaces(ctx, args, bidi)
	case *api.Peek:
		n.handlePeek(ctx, args, bidi)
	case *api.Produce:
		n.handleProduce(ctx, args, bidi)
	case *api.SubscribeToSegmentStatus:
		n.handleSubscribe(ctx, args, bidi)
	default:
		bidi.Close(fmt.Errorf("invalid request msg type: %T", envelope.Content))
	}
}

func (n *defaultNode) handleGetSpaces(ctx context.Context, _ *api.GetSpaces, bidi api.BidiStream) {
	enumerator := n.store.GetSpaces(ctx)
	streamNames(ctx, enumerator, bidi)
}

func (n *defaultNode) handleConsumeSpace(ctx context.Context, args *api.ConsumeSpace, bidi api.BidiStream) {
	enumerator := n.store.ConsumeSpace(ctx, args)
	streamEntries(ctx, enumerator, bidi)
}

func (n *defaultNode) handleGetSegments(ctx context.Context, args *api.GetSegments, bidi api.BidiStream) {
	enumerator := n.store.GetSegments(ctx, args.Space)
	streamNames(ctx, enumerator, bidi)
}

func (n *defaultNode) handleConsumeSegment(ctx context.Context, args *api.ConsumeSegment, bidi api.BidiStream) {
	enumerator := n.store.ConsumeSegment(ctx, args)
	streamEntries(ctx, enumerator, bidi)
}

func (n *defaultNode) handlePeek(ctx context.Context, args *api.Peek, bidi api.BidiStream) {
	if !checkContext(ctx, bidi) {
		return
	}
	entry, err := n.store.Peek(ctx, args.Space, args.Segment)
	if err != nil {
		bidi.CloseSend(err)
		return
	}

	if entry == nil {
		entry = &api.Entry{
			Space:   args.Space,
			Segment: args.Segment,
		}
	}

	if err := bidi.Encode(entry); err != nil {
		bidi.CloseSend(err)
		return
	}
	bidi.CloseSend(nil)
}

func (n *defaultNode) handleProduce(ctx context.Context, args *api.Produce, bidi api.BidiStream) {
	slog.DebugContext(ctx, "node: handleProduce called", "space", args.Space, "segment", args.Segment, "store_id", n.storeID)
	entries := api.NewStreamEnumerator[*api.Record](bidi)
	results := n.store.Produce(ctx, args, entries)

	bus := n.getBus(ctx)

	count := 0
	err := enumerators.ForEach(results, func(result *api.SegmentStatus) error {
		count++
		slog.DebugContext(ctx, "node: received produce result", "space", result.Space, "segment", result.Segment, "first_seq", result.FirstSequence, "last_seq", result.LastSequence, "count", count)
		if err := bidi.Encode(result); err != nil {
			return err
		}
		if bus != nil {
			notification := &api.SegmentNotification{
				StoreID:       n.storeID,
				SegmentStatus: result,
			}
			if err := bus.Notify(notification); err != nil {
				slog.WarnContext(ctx, err.Error())
			}
		}
		return nil
	})
	slog.DebugContext(ctx, "node: handleProduce complete", "results_count", count, "err", err)
	if err != nil {
		slog.ErrorContext(ctx, "produce failed", "err", err)
		bidi.CloseSend(err)
		return
	}

	bidi.CloseSend(nil)
}

func (n *defaultNode) handleConsume(ctx context.Context, args *api.Consume, bidi api.BidiStream) {
	spaces := make([]enumerators.Enumerator[*api.Entry], 0, len(args.Offsets))
	for space, offset := range args.Offsets {
		spaces = append(spaces, n.store.ConsumeSpace(ctx, &api.ConsumeSpace{
			Space:        space,
			MinTimestamp: args.MinTimestamp,
			MaxTimestamp: args.MaxTimestamp,
			Offset:       offset,
		}))
	}
	enumerator := enumerators.Interleave(spaces, func(e *api.Entry) int64 { return e.Timestamp })
	streamEntries(ctx, enumerator, bidi)
}

func (n *defaultNode) handleSubscribe(ctx context.Context, args *api.SubscribeToSegmentStatus, bidi api.BidiStream) {

	if n.busFactory == nil {
		slog.WarnContext(ctx, "the message bus factory was not configured")
		bidi.CloseSend(fmt.Errorf("the message bus was not configured"))
		return
	}

	bus, err := n.busFactory.Get(ctx)
	if err != nil {
		slog.WarnContext(ctx, "failed to connect to the message bus")
		bidi.CloseSend(fmt.Errorf("failed to connect to the message bus"))
		return
	}

	route := api.GetSegmentNotificationRoute(n.storeID, args.Space)
	sub, err := messaging.Subscribe(bus, route, func(ctx context.Context, msg *api.SegmentNotification) error {

		match := args.Segment == "*" || args.Segment == msg.SegmentStatus.Segment
		if match {
			return bidi.Encode(msg.SegmentStatus)
		}
		return nil
	})

	if err != nil {
		bidi.CloseSend(err)
		return
	}

	// Clean up on bidi close or context cancellation
	go func() {
		select {
		case <-bidi.Closed():
			sub.Unsubscribe()
		case <-ctx.Done():
			sub.Unsubscribe()
			bidi.Close(ctx.Err())
		}
	}()
}

func (n *defaultNode) getBus(ctx context.Context) messaging.MessageBus {
	if n.busFactory == nil {
		return nil
	}
	bus, err := n.busFactory.Get(ctx)
	if err != nil {
		slog.WarnContext(ctx, "the message bus factory was not configured")
		return nil
	}
	return bus
}

func streamNames(ctx context.Context, enumerator enumerators.Enumerator[string], bidi api.BidiStream) {
	defer enumerator.Dispose()
	for enumerator.MoveNext() {
		if !checkContext(ctx, bidi) {
			return
		}
		name, err := enumerator.Current()
		if err != nil {
			bidi.CloseSend(err)
			return
		}
		if err := bidi.Encode(name); err != nil {
			bidi.CloseSend(err)
			return
		}
	}
	bidi.CloseSend(nil)
}

func streamEntries(ctx context.Context, enumerator enumerators.Enumerator[*api.Entry], bidi api.BidiStream) {
	defer enumerator.Dispose()
	for enumerator.MoveNext() {
		if !checkContext(ctx, bidi) {
			return
		}
		entry, err := enumerator.Current()
		if err != nil {
			bidi.CloseSend(err)
			return
		}
		if err := bidi.Encode(entry); err != nil {
			bidi.CloseSend(err)
			return
		}
	}
	bidi.CloseSend(nil)
}

func checkContext(ctx context.Context, bidi api.BidiStream) bool {
	if err := ctx.Err(); err != nil {
		bidi.CloseSend(err)
		return false
	}
	return true
}
