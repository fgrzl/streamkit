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
	"time"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/messaging"
	"github.com/fgrzl/streamkit/internal/lease"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/storage"
	"github.com/fgrzl/streamkit/pkg/telemetry"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

// Node represents a request handler that processes streaming operations
// and coordinates with storage backends.
type Node interface {
	// Handle processes incoming bidirectional stream requests.
	Handle(context.Context, api.BidiStream)
	// Close releases all resources associated with the node.
	Close()
}

// NewNode creates a new Node instance with the specified store, optional message bus factory, and lease store.
func NewNode(storeID uuid.UUID, store storage.Store, busFactory messaging.MessageBusFactory, leaseStore *lease.Store) Node {
	return &defaultNode{
		storeID:    storeID,
		store:      store,
		busFactory: busFactory,
		leaseStore: leaseStore,
	}
}

type defaultNode struct {
	storeID    uuid.UUID
	store      storage.Store
	busFactory messaging.MessageBusFactory
	leaseStore *lease.Store
}

func (n *defaultNode) Close() {
	n.store.Close()
}

func (n *defaultNode) Handle(ctx context.Context, bidi api.BidiStream) {

	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "handler panic recovered", "panic", r)
			bidi.Close(fmt.Errorf("panic: %v", r))
		}
	}()

	envelope := &polymorphic.Envelope{}
	if err := bidi.Decode(envelope); err != nil {
		bidi.Close(err)
		return
	}

	tracer := telemetry.GetTracer()
	baseAttrs := []trace.SpanStartOption{trace.WithAttributes(telemetry.WithStoreID(n.storeID))}
	var spanName string
	var handler func(context.Context, api.BidiStream)
	switch args := envelope.Content.(type) {
	case *api.Consume:
		spanName = "streamkit.server.consume"
		handler = func(ctx context.Context, b api.BidiStream) { n.handleConsume(ctx, args, b) }
	case *api.ConsumeSegment:
		spanName = "streamkit.server.consume_segment"
		baseAttrs = append(baseAttrs, trace.WithAttributes(telemetry.WithSpace(args.Space), telemetry.WithSegment(args.Segment)))
		handler = func(ctx context.Context, b api.BidiStream) { n.handleConsumeSegment(ctx, args, b) }
	case *api.ConsumeSpace:
		spanName = "streamkit.server.consume_space"
		baseAttrs = append(baseAttrs, trace.WithAttributes(telemetry.WithSpace(args.Space)))
		handler = func(ctx context.Context, b api.BidiStream) { n.handleConsumeSpace(ctx, args, b) }
	case *api.GetSegments:
		spanName = "streamkit.server.get_segments"
		baseAttrs = append(baseAttrs, trace.WithAttributes(telemetry.WithSpace(args.Space)))
		handler = func(ctx context.Context, b api.BidiStream) { n.handleGetSegments(ctx, args, b) }
	case *api.GetSpaces:
		spanName = "streamkit.server.get_spaces"
		handler = func(ctx context.Context, b api.BidiStream) { n.handleGetSpaces(ctx, args, b) }
	case *api.Peek:
		spanName = "streamkit.server.peek"
		baseAttrs = append(baseAttrs, trace.WithAttributes(telemetry.WithSpace(args.Space), telemetry.WithSegment(args.Segment)))
		handler = func(ctx context.Context, b api.BidiStream) { n.handlePeek(ctx, args, b) }
	case *api.Produce:
		spanName = "streamkit.server.produce"
		baseAttrs = append(baseAttrs, trace.WithAttributes(telemetry.WithSpace(args.Space), telemetry.WithSegment(args.Segment)))
		handler = func(ctx context.Context, b api.BidiStream) { n.handleProduce(ctx, args, b) }
	case *api.SubscribeToSegmentStatus:
		spanName = "streamkit.server.subscribe"
		baseAttrs = append(baseAttrs, trace.WithAttributes(telemetry.WithSpace(args.Space), telemetry.WithSegment(args.Segment)))
		handler = func(ctx context.Context, b api.BidiStream) { n.handleSubscribe(ctx, args, b) }
	case *api.LeaseAcquire:
		spanName = "streamkit.server.lease_acquire"
		handler = func(ctx context.Context, b api.BidiStream) { n.handleLeaseAcquire(ctx, args, b) }
	case *api.LeaseRenew:
		spanName = "streamkit.server.lease_renew"
		handler = func(ctx context.Context, b api.BidiStream) { n.handleLeaseRenew(ctx, args, b) }
	case *api.LeaseRelease:
		spanName = "streamkit.server.lease_release"
		handler = func(ctx context.Context, b api.BidiStream) { n.handleLeaseRelease(ctx, args, b) }
	default:
		bidi.Close(fmt.Errorf("invalid request msg type: %T", envelope.Content))
		return
	}
	opCtx, span := tracer.Start(ctx, spanName, baseAttrs...)
	defer span.End()
	handler(opCtx, bidi)
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

func (n *defaultNode) handleLeaseAcquire(ctx context.Context, args *api.LeaseAcquire, bidi api.BidiStream) {
	if n.leaseStore == nil {
		_ = bidi.Encode(&api.LeaseResult{Ok: false, Message: "lease store not configured"})
		bidi.CloseSend(nil)
		return
	}
	ttl := time.Duration(args.TTLSeconds) * time.Second
	if ttl <= 0 {
		_ = bidi.Encode(&api.LeaseResult{Ok: false, Message: "ttl_seconds must be positive"})
		bidi.CloseSend(nil)
		return
	}
	ok := n.leaseStore.Acquire(args.Key, args.Holder, ttl)
	if err := bidi.Encode(&api.LeaseResult{Ok: ok}); err != nil {
		bidi.CloseSend(err)
		return
	}
	bidi.CloseSend(nil)
}

func (n *defaultNode) handleLeaseRenew(ctx context.Context, args *api.LeaseRenew, bidi api.BidiStream) {
	if n.leaseStore == nil {
		_ = bidi.Encode(&api.LeaseResult{Ok: false, Message: "lease store not configured"})
		bidi.CloseSend(nil)
		return
	}
	ttl := time.Duration(args.TTLSeconds) * time.Second
	if ttl <= 0 {
		_ = bidi.Encode(&api.LeaseResult{Ok: false, Message: "ttl_seconds must be positive"})
		bidi.CloseSend(nil)
		return
	}
	ok := n.leaseStore.Renew(args.Key, args.Holder, ttl)
	if err := bidi.Encode(&api.LeaseResult{Ok: ok}); err != nil {
		bidi.CloseSend(err)
		return
	}
	bidi.CloseSend(nil)
}

func (n *defaultNode) handleLeaseRelease(ctx context.Context, args *api.LeaseRelease, bidi api.BidiStream) {
	if n.leaseStore == nil {
		_ = bidi.Encode(&api.LeaseResult{Ok: false, Message: "lease store not configured"})
		bidi.CloseSend(nil)
		return
	}
	ok := n.leaseStore.Release(args.Key, args.Holder)
	if err := bidi.Encode(&api.LeaseResult{Ok: ok}); err != nil {
		bidi.CloseSend(err)
		return
	}
	bidi.CloseSend(nil)
}

func (n *defaultNode) handleProduce(ctx context.Context, args *api.Produce, bidi api.BidiStream) {
	entries := api.NewStreamEnumerator[*api.Record](bidi)
	results := n.store.Produce(ctx, args, entries)

	bus := n.getBus(ctx)

	count := 0
	err := enumerators.ForEach(results, func(result *api.SegmentStatus) error {
		count++
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
	// Issue #6 FIX: Add panic recovery to cleanup goroutine
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "subscription cleanup goroutine panic", "err", r)
			}
		}()
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
