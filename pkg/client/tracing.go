package client

import (
	"context"
	"sync"
	"time"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit/pkg/api"
	"github.com/fgrzl/streamkit/pkg/telemetry"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// NewClientWithTracing wraps a Client with OpenTelemetry tracing.
// Each operation will create a span with relevant attributes and error recording.
// Use this in place of NewClient when you want automatic tracing.
func NewClientWithTracing(provider api.BidiStreamProvider) Client {
	return NewClientWithTracingAndHandlerTimeout(provider, 30*time.Second)
}

// NewClientWithTracingAndHandlerTimeout wraps a Client with tracing and custom handler timeout.
func NewClientWithTracingAndHandlerTimeout(provider api.BidiStreamProvider, handlerTimeout time.Duration) Client {
	baseClient := NewClientWithHandlerTimeout(provider, handlerTimeout)
	return &tracingClient{
		client: baseClient,
		tracer: telemetry.GetTracer(),
	}
}

// tracingClient wraps a Client with OpenTelemetry instrumentation.
type tracingClient struct {
	client Client
	tracer trace.Tracer
}

// operationContext starts a new root span (new trace) for this operation so each call gets its own
// trace and does not grow unbounded span chains when the same ctx is reused for many operations.
// The returned context is cancelled when parent ctx is cancelled. Caller must defer cancel() and span.End().
func (c *tracingClient) operationContext(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span, context.CancelFunc) {
	opCtx, span := c.tracer.Start(context.Background(), spanName, opts...)
	bound, cancelBound := context.WithCancel(opCtx)
	if requestID, ok := telemetry.RequestIDFromContext(ctx); ok {
		bound = telemetry.WithRequestIDContext(bound, requestID)
	}
	done := make(chan struct{})
	var cancelOnce sync.Once
	go func() {
		select {
		case <-ctx.Done():
			cancelBound()
		case <-done:
		}
	}()
	cancel := func() {
		cancelOnce.Do(func() {
			close(done)
			cancelBound()
		})
	}
	return bound, span, cancel
}

// GetSpaces returns an enumerator of space names with tracing (stream setup span only).
func (c *tracingClient) GetSpaces(ctx context.Context, storeID uuid.UUID) enumerators.Enumerator[string] {
	requestID := generateOrGetRequestID(ctx)
	opCtx, span, cancel := c.operationContext(ctx, "streamkit.client.get_spaces",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			telemetry.WithRequestID(requestID),
		))
	defer func() { cancel(); span.End() }()
	return c.client.GetSpaces(opCtx, storeID)
}

// GetSegments returns an enumerator of segment names with tracing (stream setup span only).
func (c *tracingClient) GetSegments(ctx context.Context, storeID uuid.UUID, space string) enumerators.Enumerator[string] {
	requestID := generateOrGetRequestID(ctx)
	opCtx, span, cancel := c.operationContext(ctx, "streamkit.client.get_segments",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			telemetry.WithSpace(space),
			telemetry.WithRequestID(requestID),
		))
	defer func() { cancel(); span.End() }()
	return c.client.GetSegments(opCtx, storeID, space)
}

// Peek returns the latest entry with tracing.
func (c *tracingClient) Peek(ctx context.Context, storeID uuid.UUID, space, segment string) (*Entry, error) {
	requestID := generateOrGetRequestID(ctx)
	opCtx, span, cancel := c.operationContext(ctx, "streamkit.client.peek",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			telemetry.WithSpace(space),
			telemetry.WithSegment(segment),
			telemetry.WithRequestID(requestID),
		))
	defer func() { cancel(); span.End() }()

	entry, err := c.client.Peek(opCtx, storeID, space, segment)
	if err != nil {
		telemetry.RecordError(span, err)
		return nil, err
	}

	if entry != nil {
		span.SetAttributes(
			telemetry.WithSequenceRange(entry.Sequence, entry.Sequence)[0],
			attribute.Int64("streamkit.timestamp", entry.Timestamp),
		)
	}

	return entry, nil
}

// Consume reads entries from multiple spaces with tracing (stream setup span only).
func (c *tracingClient) Consume(ctx context.Context, storeID uuid.UUID, args *Consume) enumerators.Enumerator[*Entry] {
	requestID := generateOrGetRequestID(ctx)
	opCtx, span, cancel := c.operationContext(ctx, "streamkit.client.consume",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			telemetry.WithRequestID(requestID),
		))
	defer func() { cancel(); span.End() }()
	return c.client.Consume(opCtx, storeID, args)
}

// ConsumeSpace reads entries from all segments with tracing (stream setup span only).
func (c *tracingClient) ConsumeSpace(ctx context.Context, storeID uuid.UUID, args *ConsumeSpace) enumerators.Enumerator[*Entry] {
	requestID := generateOrGetRequestID(ctx)
	opCtx, span, cancel := c.operationContext(ctx, "streamkit.client.consume_space",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			telemetry.WithSpace(args.Space),
			telemetry.WithRequestID(requestID),
		))
	defer func() { cancel(); span.End() }()
	return c.client.ConsumeSpace(opCtx, storeID, args)
}

// ConsumeSegment reads entries from a specific segment with tracing (stream setup span only).
func (c *tracingClient) ConsumeSegment(ctx context.Context, storeID uuid.UUID, args *ConsumeSegment) enumerators.Enumerator[*Entry] {
	requestID := generateOrGetRequestID(ctx)
	baseAttrs := []attribute.KeyValue{
		telemetry.WithStoreID(storeID),
		telemetry.WithSpace(args.Space),
		telemetry.WithSegment(args.Segment),
		telemetry.WithRequestID(requestID),
	}
	seqAttrs := telemetry.WithSequenceRange(args.MinSequence, args.MaxSequence)
	allAttrs := append(baseAttrs, seqAttrs...)
	opCtx, span, cancel := c.operationContext(ctx, "streamkit.client.consume_segment",
		trace.WithAttributes(allAttrs...))
	defer func() { cancel(); span.End() }()
	return c.client.ConsumeSegment(opCtx, storeID, args)
}

// Produce writes records to a segment with tracing (stream setup span only).
func (c *tracingClient) Produce(ctx context.Context, storeID uuid.UUID, space, segment string, entries enumerators.Enumerator[*Record]) enumerators.Enumerator[*SegmentStatus] {
	requestID := generateOrGetRequestID(ctx)
	opCtx, span, cancel := c.operationContext(ctx, "streamkit.client.produce",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			telemetry.WithSpace(space),
			telemetry.WithSegment(segment),
			telemetry.WithRequestID(requestID),
		))
	defer func() { cancel(); span.End() }()
	return c.client.Produce(opCtx, storeID, space, segment, entries)
}

// Publish writes a single record with tracing.
func (c *tracingClient) Publish(ctx context.Context, storeID uuid.UUID, space, segment string, payload []byte, metadata map[string]string) error {
	requestID := generateOrGetRequestID(ctx)
	opCtx, span, cancel := c.operationContext(ctx, "streamkit.client.publish",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			telemetry.WithSpace(space),
			telemetry.WithSegment(segment),
			telemetry.WithPayloadSize(len(payload)),
			telemetry.WithRequestID(requestID),
		))
	defer func() { cancel(); span.End() }()

	err := c.client.Publish(opCtx, storeID, space, segment, payload, metadata)
	if err != nil {
		telemetry.RecordError(span, err)
		return err
	}

	return nil
}

// SubscribeToSegment subscribes to segment status updates with tracing (setup span only).
func (c *tracingClient) SubscribeToSegment(ctx context.Context, storeID uuid.UUID, space, segment string, handler func(*SegmentStatus)) (api.Subscription, error) {
	requestID := generateOrGetRequestID(ctx)
	opCtx, span, cancel := c.operationContext(ctx, "streamkit.client.subscribe_segment",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			telemetry.WithSpace(space),
			telemetry.WithSegment(segment),
			telemetry.WithRequestID(requestID),
		))
	defer func() { cancel(); span.End() }()
	sub, err := c.client.SubscribeToSegment(opCtx, storeID, space, segment, handler)
	if err != nil {
		telemetry.RecordError(span, err)
		return nil, err
	}
	return sub, nil
}

// SubscribeToSpace subscribes to space status updates with tracing (setup span only).
func (c *tracingClient) SubscribeToSpace(ctx context.Context, storeID uuid.UUID, space string, handler func(*SegmentStatus)) (api.Subscription, error) {
	requestID := generateOrGetRequestID(ctx)
	opCtx, span, cancel := c.operationContext(ctx, "streamkit.client.subscribe_space",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			telemetry.WithSpace(space),
			telemetry.WithRequestID(requestID),
		))
	defer func() { cancel(); span.End() }()
	sub, err := c.client.SubscribeToSpace(opCtx, storeID, space, handler)
	if err != nil {
		telemetry.RecordError(span, err)
		return nil, err
	}
	return sub, nil
}

// GetSubscriptionStatus returns subscription status.
func (c *tracingClient) GetSubscriptionStatus(id string) *SubscriptionStatus {
	return c.client.GetSubscriptionStatus(id)
}

// WithLease runs the callback with a lease-held context, with tracing.
func (c *tracingClient) WithLease(ctx context.Context, storeID uuid.UUID, key string, ttl time.Duration, fn func(context.Context) error) error {
	requestID := generateOrGetRequestID(ctx)
	opCtx, span, cancel := c.operationContext(ctx, "streamkit.client.with_lease",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			attribute.String("streamkit.lease.key", key),
			attribute.Int64("streamkit.lease.ttl_sec", int64(ttl.Seconds())),
			telemetry.WithRequestID(requestID),
		))
	defer func() { cancel(); span.End() }()

	err := c.client.WithLease(opCtx, storeID, key, ttl, fn)
	if err != nil {
		telemetry.RecordError(span, err)
		return err
	}
	return nil
}

// Close gracefully shuts down the client.
func (c *tracingClient) Close() error {
	_, span := c.tracer.Start(context.Background(), "streamkit.client.close")
	defer span.End()

	err := c.client.Close()
	if err != nil {
		telemetry.RecordError(span, err)
		return err
	}

	return nil
}

// generateOrGetRequestID extracts or generates a request ID for context.
func generateOrGetRequestID(ctx context.Context) uuid.UUID {
	if id, ok := telemetry.RequestIDFromContext(ctx); ok {
		return id
	}
	return uuid.New()
}
