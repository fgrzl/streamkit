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
	"sort"
	"sync"
	"time"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/json/polymorphic"
	"github.com/fgrzl/streamkit/internal/lease"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/bus"
	"github.com/fgrzl/streamkit/pkg/storage"
	"github.com/fgrzl/streamkit/pkg/telemetry"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

const (
	minSubscriptionHeartbeatIntervalSeconds = 1
	maxSubscriptionHeartbeatIntervalSeconds = 300
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
func NewNode(storeID uuid.UUID, store storage.Store, busFactory bus.MessageBusFactory, leaseStore *lease.Store) Node {
	return &defaultNode{
		storeID:                 storeID,
		store:                   store,
		busFactory:              busFactory,
		leaseStore:              leaseStore,
		subscriptionRouters:     make(map[string]*spaceSubscriptionRouter),
		notifyFailureThreshold:  defaultNotifyFailureThreshold,
		notifyCircuitOpenWindow: defaultNotifyCircuitOpenWindow,
	}
}

type defaultNode struct {
	storeID    uuid.UUID
	store      storage.Store
	busFactory bus.MessageBusFactory
	leaseStore *lease.Store

	subscriptionRoutersMu sync.Mutex
	subscriptionRouters   map[string]*spaceSubscriptionRouter

	notifyMu                sync.Mutex
	notifyFailureCount      int
	notifyCircuitOpenUntil  time.Time
	notifyFailureThreshold  int
	notifyCircuitOpenWindow time.Duration
}

const maxLeaseTTL = 24 * time.Hour
const defaultNotifyFailureThreshold = 5
const defaultNotifyCircuitOpenWindow = time.Minute

func (n *defaultNode) Close() {
	n.subscriptionRoutersMu.Lock()
	routers := make([]*spaceSubscriptionRouter, 0, len(n.subscriptionRouters))
	for _, router := range n.subscriptionRouters {
		routers = append(routers, router)
	}
	n.subscriptionRouters = make(map[string]*spaceSubscriptionRouter)
	n.subscriptionRoutersMu.Unlock()

	for _, router := range routers {
		router.close()
	}
	n.store.Close()
}

func (n *defaultNode) Handle(ctx context.Context, bidi api.BidiStream) {

	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "server: handler panic recovered",
				logContextFields(ctx, n.storeID, slog.Any("panic", r))...)
			bidi.Close(fmt.Errorf("panic: %v", r))
		}
	}()

	envelope := &polymorphic.Envelope{}
	if err := bidi.Decode(envelope); err != nil {
		slog.WarnContext(ctx, "server: failed to decode request envelope",
			logContextFields(ctx, n.storeID, "err", err)...)
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
		slog.WarnContext(ctx, "server: invalid request message type",
			logContextFields(ctx, n.storeID, slog.String("type", fmt.Sprintf("%T", envelope.Content)))...)
		bidi.Close(fmt.Errorf("invalid request msg type: %T", envelope.Content))
		return
	}
	// One span per message: covers this request only (no long-lived chains).
	opCtx, span := tracer.Start(ctx, spanName, baseAttrs...)
	defer span.End()
	handler(opCtx, bidi)
}

func (n *defaultNode) handleGetSpaces(ctx context.Context, _ *api.GetSpaces, bidi api.BidiStream) {
	enumerator := n.store.GetSpaces(ctx)
	fields := logContextFields(ctx, n.storeID)
	runAsyncStream(ctx, bidi, fields, func() {
		streamNames(ctx, "get spaces", enumerator, bidi, fields...)
	})
}

func (n *defaultNode) handleConsumeSpace(ctx context.Context, args *api.ConsumeSpace, bidi api.BidiStream) {
	enumerator := n.store.ConsumeSpace(ctx, args)
	fields := logContextFields(ctx, n.storeID, slog.String("space", args.Space))
	runAsyncStream(ctx, bidi, fields, func() {
		streamEntries(ctx, "consume space", enumerator, bidi, fields...)
	})
}

func (n *defaultNode) handleGetSegments(ctx context.Context, args *api.GetSegments, bidi api.BidiStream) {
	enumerator := n.store.GetSegments(ctx, args.Space)
	fields := logContextFields(ctx, n.storeID, slog.String("space", args.Space))
	runAsyncStream(ctx, bidi, fields, func() {
		streamNames(ctx, "get segments", enumerator, bidi, fields...)
	})
}

func (n *defaultNode) handleConsumeSegment(ctx context.Context, args *api.ConsumeSegment, bidi api.BidiStream) {
	enumerator := n.store.ConsumeSegment(ctx, args)
	fields := logContextFields(ctx, n.storeID,
		slog.String("space", args.Space),
		slog.String("segment", args.Segment))
	runAsyncStream(ctx, bidi, fields, func() {
		streamEntries(ctx, "consume segment", enumerator, bidi, fields...)
	})
}

func (n *defaultNode) handlePeek(ctx context.Context, args *api.Peek, bidi api.BidiStream) {
	if !checkContext(ctx, bidi) {
		return
	}
	entry, err := n.store.Peek(ctx, args.Space, args.Segment)
	if err != nil {
		telemetry.RecordError(trace.SpanFromContext(ctx), err)
		slog.ErrorContext(ctx, "server: peek failed",
			logContextFields(ctx, n.storeID,
				slog.String("space", args.Space),
				slog.String("segment", args.Segment),
				"err", err)...)
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
		telemetry.RecordError(trace.SpanFromContext(ctx), err)
		slog.WarnContext(ctx, "server: failed to encode peek response",
			logContextFields(ctx, n.storeID,
				slog.String("space", args.Space),
				slog.String("segment", args.Segment),
				"err", err)...)
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
	if args.Key == "" {
		_ = bidi.Encode(&api.LeaseResult{Ok: false, Message: "lease key must not be empty"})
		bidi.CloseSend(nil)
		return
	}
	if args.Holder == "" {
		_ = bidi.Encode(&api.LeaseResult{Ok: false, Message: "lease holder must not be empty"})
		bidi.CloseSend(nil)
		return
	}
	ttl := time.Duration(args.TTLSeconds) * time.Second
	if ttl <= 0 {
		_ = bidi.Encode(&api.LeaseResult{Ok: false, Message: "ttl_seconds must be positive"})
		bidi.CloseSend(nil)
		return
	}
	if ttl > maxLeaseTTL {
		_ = bidi.Encode(&api.LeaseResult{Ok: false, Message: "ttl_seconds exceeds maximum of 86400"})
		bidi.CloseSend(nil)
		return
	}
	ok := n.leaseStore.Acquire(args.Key, args.Holder, ttl)
	if err := bidi.Encode(&api.LeaseResult{Ok: ok}); err != nil {
		telemetry.RecordError(trace.SpanFromContext(ctx), err)
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
	if args.Key == "" {
		_ = bidi.Encode(&api.LeaseResult{Ok: false, Message: "lease key must not be empty"})
		bidi.CloseSend(nil)
		return
	}
	if args.Holder == "" {
		_ = bidi.Encode(&api.LeaseResult{Ok: false, Message: "lease holder must not be empty"})
		bidi.CloseSend(nil)
		return
	}
	ttl := time.Duration(args.TTLSeconds) * time.Second
	if ttl <= 0 {
		_ = bidi.Encode(&api.LeaseResult{Ok: false, Message: "ttl_seconds must be positive"})
		bidi.CloseSend(nil)
		return
	}
	if ttl > maxLeaseTTL {
		_ = bidi.Encode(&api.LeaseResult{Ok: false, Message: "ttl_seconds exceeds maximum of 86400"})
		bidi.CloseSend(nil)
		return
	}
	ok := n.leaseStore.Renew(args.Key, args.Holder, ttl)
	if err := bidi.Encode(&api.LeaseResult{Ok: ok}); err != nil {
		telemetry.RecordError(trace.SpanFromContext(ctx), err)
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
	if args.Key == "" {
		_ = bidi.Encode(&api.LeaseResult{Ok: false, Message: "lease key must not be empty"})
		bidi.CloseSend(nil)
		return
	}
	if args.Holder == "" {
		_ = bidi.Encode(&api.LeaseResult{Ok: false, Message: "lease holder must not be empty"})
		bidi.CloseSend(nil)
		return
	}
	ok := n.leaseStore.Release(args.Key, args.Holder)
	if err := bidi.Encode(&api.LeaseResult{Ok: ok}); err != nil {
		telemetry.RecordError(trace.SpanFromContext(ctx), err)
		bidi.CloseSend(err)
		return
	}
	bidi.CloseSend(nil)
}

func (n *defaultNode) handleProduce(ctx context.Context, args *api.Produce, bidi api.BidiStream) {
	entries := api.NewStreamEnumerator[*api.Record](bidi)
	results := n.store.Produce(ctx, args, entries)

	bus := n.getBus(ctx)
	fields := logContextFields(ctx, n.storeID,
		slog.String("space", args.Space),
		slog.String("segment", args.Segment))

	runAsyncStream(ctx, bidi, fields, func() {
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
				if err := n.publishSegmentNotification(ctx, bus, notification); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			slog.ErrorContext(ctx, "server: produce failed",
				appendLogFields(fields,
					slog.Int("status_count", count),
					"err", err)...)
			bidi.CloseSend(err)
			return
		}

		bidi.CloseSend(nil)
	})
}

func (n *defaultNode) publishSegmentNotification(ctx context.Context, messageBus bus.MessageBus, notification *api.SegmentNotification) error {
	if messageBus == nil || notification == nil || notification.SegmentStatus == nil {
		return nil
	}

	now := time.Now()
	if err := n.checkNotifyCircuit(now); err != nil {
		slog.WarnContext(ctx, "server: segment notification circuit open",
			logContextFields(ctx, n.storeID,
				slog.String("space", notification.SegmentStatus.Space),
				slog.String("segment", notification.SegmentStatus.Segment),
				slog.Uint64("last_sequence", notification.SegmentStatus.LastSequence),
				"err", err)...)
		return err
	}

	if err := messageBus.Notify(notification); err != nil {
		telemetry.RecordError(trace.SpanFromContext(ctx), err)
		count, opened, retryAfter := n.recordNotifyFailure(now)
		fields := logContextFields(ctx, n.storeID,
			slog.String("space", notification.SegmentStatus.Space),
			slog.String("segment", notification.SegmentStatus.Segment),
			slog.Uint64("last_sequence", notification.SegmentStatus.LastSequence),
			slog.Int("consecutive_failures", count),
			"err", err)
		if opened {
			circuitErr := fmt.Errorf("segment notification circuit open after %d consecutive failures; retry after %s: %w", count, retryAfter.Round(time.Second), err)
			slog.ErrorContext(ctx, "server: failed to publish segment notification; circuit opened",
				append(fields, slog.Duration("retry_after", retryAfter))...)
			return circuitErr
		}
		slog.WarnContext(ctx, "server: failed to publish segment notification", fields...)
		return nil
	}

	if recovered := n.recordNotifySuccess(); recovered {
		slog.InfoContext(ctx, "server: segment notification publishing recovered",
			logContextFields(ctx, n.storeID,
				slog.String("space", notification.SegmentStatus.Space),
				slog.String("segment", notification.SegmentStatus.Segment),
				slog.Uint64("last_sequence", notification.SegmentStatus.LastSequence))...)
	}
	return nil
}

func (n *defaultNode) checkNotifyCircuit(now time.Time) error {
	n.notifyMu.Lock()
	defer n.notifyMu.Unlock()
	if n.notifyCircuitOpenUntil.After(now) {
		return fmt.Errorf("segment notification circuit open: retry after %s", n.notifyCircuitOpenUntil.Sub(now).Round(time.Second))
	}
	if !n.notifyCircuitOpenUntil.IsZero() {
		n.notifyCircuitOpenUntil = time.Time{}
		n.notifyFailureCount = 0
	}
	return nil
}

func (n *defaultNode) recordNotifyFailure(now time.Time) (count int, opened bool, retryAfter time.Duration) {
	n.notifyMu.Lock()
	defer n.notifyMu.Unlock()
	n.notifyFailureCount++
	count = n.notifyFailureCount
	threshold := n.notifyFailureThreshold
	if threshold <= 0 {
		threshold = defaultNotifyFailureThreshold
	}
	window := n.notifyCircuitOpenWindow
	if window <= 0 {
		window = defaultNotifyCircuitOpenWindow
	}
	if count >= threshold {
		n.notifyCircuitOpenUntil = now.Add(window)
		opened = true
		retryAfter = window
	}
	return count, opened, retryAfter
}

func (n *defaultNode) recordNotifySuccess() bool {
	n.notifyMu.Lock()
	defer n.notifyMu.Unlock()
	recovered := n.notifyFailureCount > 0 || !n.notifyCircuitOpenUntil.IsZero()
	n.notifyFailureCount = 0
	n.notifyCircuitOpenUntil = time.Time{}
	return recovered
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
	fields := logContextFields(ctx, n.storeID, slog.Int("space_count", len(args.Offsets)))
	runAsyncStream(ctx, bidi, fields, func() {
		streamEntries(ctx, "consume", enumerator, bidi, fields...)
	})
}

func (n *defaultNode) handleSubscribe(ctx context.Context, args *api.SubscribeToSegmentStatus, bidi api.BidiStream) {
	span := trace.SpanFromContext(ctx)
	if n.busFactory == nil {
		err := fmt.Errorf("the message bus was not configured")
		telemetry.RecordError(span, err)
		slog.WarnContext(ctx, "server: message bus factory was not configured",
			logContextFields(ctx, n.storeID,
				slog.String("space", args.Space),
				slog.String("segment", args.Segment))...)
		bidi.CloseSend(err)
		return
	}

	subscriber, err := n.registerSubscriptionTarget(ctx, args.Space, args.Segment, bidi)
	if err != nil {
		telemetry.RecordError(span, err)
		slog.WarnContext(ctx, "server: failed to initialize subscription router",
			logContextFields(ctx, n.storeID,
				slog.String("space", args.Space),
				slog.String("segment", args.Segment),
				"err", err)...)
		bidi.CloseSend(fmt.Errorf("failed to connect to the message bus"))
		return
	}

	subCtx, cancelSub := context.WithCancel(ctx)
	var cleanupOnce sync.Once
	cleanup := func(closeErr error, closeBidi bool) {
		cleanupOnce.Do(func() {
			subscriber.router.unregisterSubscriber(subscriber)
			cancelSub()
			if closeBidi {
				bidi.Close(closeErr)
			}
		})
	}

	snapshotStatuses, err := n.collectSubscriptionSnapshots(ctx, args)
	if err != nil {
		telemetry.RecordError(span, err)
		slog.WarnContext(ctx, "server: failed to collect subscription snapshot",
			logContextFields(ctx, n.storeID,
				slog.String("space", args.Space),
				slog.String("segment", args.Segment),
				"err", err)...)
		cleanup(err, true)
		return
	}

	if err := subscriber.emitSnapshot(snapshotStatuses); err != nil {
		telemetry.RecordError(span, err)
		slog.WarnContext(ctx, "server: failed to encode subscription snapshot",
			logContextFields(ctx, n.storeID,
				slog.String("space", args.Space),
				slog.String("segment", args.Segment),
				"err", err)...)
		cleanup(err, true)
		return
	}

	if heartbeatSeconds := clampSubscriptionHeartbeatIntervalSeconds(args.HeartbeatIntervalSeconds); heartbeatSeconds > 0 {
		go n.streamSubscriptionHeartbeats(subCtx, args.Space, args.Segment, heartbeatSeconds, bidi)
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(subCtx, "server: subscription cleanup goroutine panic",
					logContextFields(subCtx, n.storeID,
						slog.String("space", args.Space),
						slog.String("segment", args.Segment),
						slog.Any("panic", r))...)
			}
		}()
		select {
		case <-bidi.Closed():
			cleanup(nil, false)
		case <-subCtx.Done():
			cleanup(subCtx.Err(), true)
		}
	}()
}

func clampSubscriptionHeartbeatIntervalSeconds(seconds int64) int64 {
	if seconds <= 0 {
		return 0
	}
	if seconds < minSubscriptionHeartbeatIntervalSeconds {
		return minSubscriptionHeartbeatIntervalSeconds
	}
	if seconds > maxSubscriptionHeartbeatIntervalSeconds {
		return maxSubscriptionHeartbeatIntervalSeconds
	}
	return seconds
}

func (n *defaultNode) streamSubscriptionHeartbeats(ctx context.Context, space, segment string, heartbeatSeconds int64, bidi api.BidiStream) {
	interval := time.Duration(clampSubscriptionHeartbeatIntervalSeconds(heartbeatSeconds)) * time.Second
	if interval <= 0 {
		return
	}

	sendHeartbeat := func() error {
		return bidi.Encode(&api.SegmentStatus{
			Space:     space,
			Segment:   segment,
			Heartbeat: true,
		})
	}

	if err := sendHeartbeat(); err != nil {
		bidi.Close(err)
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-bidi.Closed():
			return
		case <-ticker.C:
			if err := sendHeartbeat(); err != nil {
				bidi.Close(err)
				return
			}
		}
	}
}

func (n *defaultNode) collectSubscriptionSnapshots(ctx context.Context, args *api.SubscribeToSegmentStatus) ([]*api.SegmentStatus, error) {
	if args == nil {
		return nil, nil
	}
	if args.Segment != "*" {
		status, err := n.segmentSnapshot(ctx, args.Space, args.Segment)
		if err != nil || status == nil {
			return nil, err
		}
		return []*api.SegmentStatus{status}, nil
	}

	segments, err := enumerators.ToSlice(n.store.GetSegments(ctx, args.Space))
	if err != nil {
		return nil, err
	}
	sort.Strings(segments)

	statuses := make([]*api.SegmentStatus, 0, len(segments))
	for _, segment := range segments {
		status, err := n.segmentSnapshot(ctx, args.Space, segment)
		if err != nil {
			return nil, err
		}
		if status != nil {
			statuses = append(statuses, status)
		}
	}
	return statuses, nil
}

func (n *defaultNode) segmentSnapshot(ctx context.Context, space, segment string) (*api.SegmentStatus, error) {
	if statusStore, ok := n.store.(storage.SegmentStatusStore); ok {
		return statusStore.GetSegmentStatus(ctx, space, segment)
	}

	lastEntry, err := n.store.Peek(ctx, space, segment)
	if err != nil {
		return nil, err
	}
	if lastEntry == nil || lastEntry.Sequence == 0 {
		return nil, nil
	}

	firstEntry, err := firstSegmentEntry(ctx, n.store, space, segment)
	if err != nil {
		return nil, err
	}
	if firstEntry == nil {
		firstEntry = lastEntry
	}

	return &api.SegmentStatus{
		Space:          space,
		Segment:        segment,
		FirstSequence:  firstEntry.Sequence,
		FirstTimestamp: firstEntry.Timestamp,
		LastSequence:   lastEntry.Sequence,
		LastTimestamp:  lastEntry.Timestamp,
	}, nil
}

func firstSegmentEntry(ctx context.Context, store storage.Store, space, segment string) (*api.Entry, error) {
	if store == nil {
		return nil, nil
	}

	enum := store.ConsumeSegment(ctx, &api.ConsumeSegment{
		Space:       space,
		Segment:     segment,
		MinSequence: 1,
	})
	if enum == nil {
		return nil, nil
	}
	defer enum.Dispose()

	if !enum.MoveNext() {
		if err := enum.Err(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	return enum.Current()
}

func cloneSegmentStatus(status *api.SegmentStatus) *api.SegmentStatus {
	if status == nil {
		return nil
	}
	cloned := *status
	return &cloned
}

func sortedBufferedStatuses(buffered map[string]*api.SegmentStatus) []*api.SegmentStatus {
	if len(buffered) == 0 {
		return nil
	}

	segments := make([]string, 0, len(buffered))
	for segment := range buffered {
		segments = append(segments, segment)
	}
	sort.Strings(segments)

	statuses := make([]*api.SegmentStatus, 0, len(segments))
	for _, segment := range segments {
		statuses = append(statuses, buffered[segment])
	}
	return statuses
}

func (n *defaultNode) getBus(ctx context.Context) bus.MessageBus {
	if n.busFactory == nil {
		return nil
	}
	messageBus, err := n.busFactory.Get(ctx)
	if err != nil {
		slog.WarnContext(ctx, "server: failed to get message bus",
			logContextFields(ctx, n.storeID, "err", err)...)
		return nil
	}
	return messageBus
}

func runAsyncStream(ctx context.Context, bidi api.BidiStream, fields []any, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "server: async stream handler panic",
					appendLogFields(fields, slog.Any("panic", r))...)
				bidi.Close(fmt.Errorf("panic: %v", r))
			}
		}()
		fn()
	}()
}

func streamNames(ctx context.Context, operation string, enumerator enumerators.Enumerator[string], bidi api.BidiStream, fields ...any) {
	defer enumerator.Dispose()
	for enumerator.MoveNext() {
		if !checkContext(ctx, bidi) {
			return
		}
		name, err := enumerator.Current()
		if err != nil {
			slog.WarnContext(ctx, "server: "+operation+" enumerator failed",
				appendLogFields(fields, "err", err)...)
			bidi.CloseSend(err)
			return
		}
		if err := bidi.Encode(name); err != nil {
			slog.WarnContext(ctx, "server: "+operation+" response encode failed",
				appendLogFields(fields, "err", err)...)
			bidi.CloseSend(err)
			return
		}
	}
	bidi.CloseSend(nil)
}

func streamEntries(ctx context.Context, operation string, enumerator enumerators.Enumerator[*api.Entry], bidi api.BidiStream, fields ...any) {
	defer enumerator.Dispose()
	for enumerator.MoveNext() {
		if !checkContext(ctx, bidi) {
			return
		}
		entry, err := enumerator.Current()
		if err != nil {
			slog.WarnContext(ctx, "server: "+operation+" enumerator failed",
				appendLogFields(fields, "err", err)...)
			bidi.CloseSend(err)
			return
		}
		if err := bidi.Encode(entry); err != nil {
			slog.WarnContext(ctx, "server: "+operation+" response encode failed",
				appendLogFields(fields, "err", err)...)
			bidi.CloseSend(err)
			return
		}
	}
	bidi.CloseSend(nil)
}

func logContextFields(ctx context.Context, storeID uuid.UUID, extras ...any) []any {
	fields := []any{slog.String("store_id", storeID.String())}
	if channelID, ok := ChannelIDFromContext(ctx); ok {
		fields = append(fields, slog.String("channel_id", channelID.String()))
	}
	return append(fields, extras...)
}

func appendLogFields(fields []any, extras ...any) []any {
	out := make([]any, 0, len(fields)+len(extras))
	out = append(out, fields...)
	out = append(out, extras...)
	return out
}

func checkContext(ctx context.Context, bidi api.BidiStream) bool {
	if err := ctx.Err(); err != nil {
		bidi.CloseSend(err)
		return false
	}
	return true
}
