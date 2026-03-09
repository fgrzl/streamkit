package client

import (
	"context"
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

// GetSpaces returns an enumerator of space names with tracing.
func (c *tracingClient) GetSpaces(ctx context.Context, storeID uuid.UUID) enumerators.Enumerator[string] {
	requestID := generateOrGetRequestID(ctx)
	ctx, span := c.tracer.Start(ctx, "streamkit.client.get_spaces",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			telemetry.WithRequestID(requestID),
		))
	defer span.End()

	result := c.client.GetSpaces(ctx, storeID)

	return wrapStringEnumerator(ctx, result, span, "streamkit.client.enumerator_chunk")
}

// GetSegments returns an enumerator of segment names with tracing.
func (c *tracingClient) GetSegments(ctx context.Context, storeID uuid.UUID, space string) enumerators.Enumerator[string] {
	requestID := generateOrGetRequestID(ctx)
	ctx, span := c.tracer.Start(ctx, "streamkit.client.get_segments",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			telemetry.WithSpace(space),
			telemetry.WithRequestID(requestID),
		))
	defer span.End()

	result := c.client.GetSegments(ctx, storeID, space)

	return wrapStringEnumerator(ctx, result, span, "streamkit.client.enumerator_chunk")
}

// Peek returns the latest entry with tracing.
func (c *tracingClient) Peek(ctx context.Context, storeID uuid.UUID, space, segment string) (*Entry, error) {
	requestID := generateOrGetRequestID(ctx)
	ctx, span := c.tracer.Start(ctx, "streamkit.client.peek",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			telemetry.WithSpace(space),
			telemetry.WithSegment(segment),
			telemetry.WithRequestID(requestID),
		))
	defer span.End()

	entry, err := c.client.Peek(ctx, storeID, space, segment)
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

// Consume reads entries from multiple spaces with tracing.
func (c *tracingClient) Consume(ctx context.Context, storeID uuid.UUID, args *Consume) enumerators.Enumerator[*Entry] {
	requestID := generateOrGetRequestID(ctx)
	ctx, span := c.tracer.Start(ctx, "streamkit.client.consume",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			telemetry.WithRequestID(requestID),
		))
	defer span.End()

	result := c.client.Consume(ctx, storeID, args)

	return wrapEntryEnumerator(ctx, result, span, "streamkit.client.enumerator_chunk")
}

// ConsumeSpace reads entries from all segments with tracing.
func (c *tracingClient) ConsumeSpace(ctx context.Context, storeID uuid.UUID, args *ConsumeSpace) enumerators.Enumerator[*Entry] {
	requestID := generateOrGetRequestID(ctx)
	ctx, span := c.tracer.Start(ctx, "streamkit.client.consume_space",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			telemetry.WithSpace(args.Space),
			telemetry.WithRequestID(requestID),
		))
	defer span.End()

	result := c.client.ConsumeSpace(ctx, storeID, args)

	return wrapEntryEnumerator(ctx, result, span, "streamkit.client.enumerator_chunk")
}

// ConsumeSegment reads entries from a specific segment with tracing.
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
	ctx, span := c.tracer.Start(ctx, "streamkit.client.consume_segment",
		trace.WithAttributes(allAttrs...),
	)
	defer span.End()

	result := c.client.ConsumeSegment(ctx, storeID, args)

	return wrapEntryEnumerator(ctx, result, span, "streamkit.client.enumerator_chunk")
}

// Produce writes records to a segment with tracing.
func (c *tracingClient) Produce(ctx context.Context, storeID uuid.UUID, space, segment string, entries enumerators.Enumerator[*Record]) enumerators.Enumerator[*SegmentStatus] {
	requestID := generateOrGetRequestID(ctx)
	ctx, span := c.tracer.Start(ctx, "streamkit.client.produce",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			telemetry.WithSpace(space),
			telemetry.WithSegment(segment),
			telemetry.WithRequestID(requestID),
		))
	defer span.End()

	result := c.client.Produce(ctx, storeID, space, segment, entries)

	return wrapSegmentStatusEnumerator(ctx, result, span, "streamkit.client.enumerator_chunk")
}

// Publish writes a single record with tracing.
func (c *tracingClient) Publish(ctx context.Context, storeID uuid.UUID, space, segment string, payload []byte, metadata map[string]string) error {
	requestID := generateOrGetRequestID(ctx)
	ctx, span := c.tracer.Start(ctx, "streamkit.client.publish",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			telemetry.WithSpace(space),
			telemetry.WithSegment(segment),
			telemetry.WithPayloadSize(len(payload)),
			telemetry.WithRequestID(requestID),
		))
	defer span.End()

	err := c.client.Publish(ctx, storeID, space, segment, payload, metadata)
	if err != nil {
		telemetry.RecordError(span, err)
		return err
	}

	return nil
}

// SubscribeToSegment subscribes to segment status updates with tracing.
func (c *tracingClient) SubscribeToSegment(ctx context.Context, storeID uuid.UUID, space, segment string, handler func(*SegmentStatus)) (api.Subscription, error) {
	requestID := generateOrGetRequestID(ctx)
	ctx, span := c.tracer.Start(ctx, "streamkit.client.subscribe_segment",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			telemetry.WithSpace(space),
			telemetry.WithSegment(segment),
			telemetry.WithRequestID(requestID),
		))
	defer span.End()

	// Wrap handler to trace invocations
	tracedHandler := func(status *SegmentStatus) {
		_, hSpan := c.tracer.Start(ctx, "streamkit.client.handler_invoke",
			trace.WithAttributes(
				telemetry.WithStoreID(storeID),
				telemetry.WithSpace(space),
				telemetry.WithSegment(segment),
			))
		defer hSpan.End()

		handler(status)
	}

	sub, err := c.client.SubscribeToSegment(ctx, storeID, space, segment, tracedHandler)
	if err != nil {
		telemetry.RecordError(span, err)
		return nil, err
	}

	span.SetAttributes(telemetry.WithRequestID(generateOrGetRequestID(ctx)))
	return sub, nil
}

// SubscribeToSpace subscribes to space status updates with tracing.
func (c *tracingClient) SubscribeToSpace(ctx context.Context, storeID uuid.UUID, space string, handler func(*SegmentStatus)) (api.Subscription, error) {
	requestID := generateOrGetRequestID(ctx)
	ctx, span := c.tracer.Start(ctx, "streamkit.client.subscribe_space",
		trace.WithAttributes(
			telemetry.WithStoreID(storeID),
			telemetry.WithSpace(space),
			telemetry.WithRequestID(requestID),
		))
	defer span.End()

	// Wrap handler to trace invocations
	tracedHandler := func(status *SegmentStatus) {
		_, hSpan := c.tracer.Start(ctx, "streamkit.client.handler_invoke",
			trace.WithAttributes(
				telemetry.WithStoreID(storeID),
				telemetry.WithSpace(space),
			))
		defer hSpan.End()

		handler(status)
	}

	sub, err := c.client.SubscribeToSpace(ctx, storeID, space, tracedHandler)
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

// Helper functions for enumerator wrapping

// wrapStringEnumerator wraps a string enumerator to emit chunk spans.
func wrapStringEnumerator(ctx context.Context, enum enumerators.Enumerator[string], parentSpan trace.Span, spanName string) enumerators.Enumerator[string] {
	tracer := telemetry.GetTracer()
	chunkCount := 0

	return enumerators.Map(enum, func(s string) (string, error) {
		chunkCount++
		if chunkCount == 1 {
			_, span := tracer.Start(ctx, spanName,
				trace.WithAttributes(
					attribute.Int("streamkit.chunk_num", chunkCount),
				))
			defer span.End()
		}
		return s, nil
	})
}

// wrapEntryEnumerator wraps an entry enumerator to emit chunk spans.
func wrapEntryEnumerator(ctx context.Context, enum enumerators.Enumerator[*Entry], parentSpan trace.Span, spanName string) enumerators.Enumerator[*Entry] {
	tracer := telemetry.GetTracer()
	chunkCount := 0
	chunkSize := 0

	return enumerators.Map(enum, func(e *Entry) (*Entry, error) {
		chunkCount++
		chunkSize += len(e.Payload)

		_, span := tracer.Start(ctx, spanName,
			trace.WithAttributes(
				attribute.Int("streamkit.chunk_num", chunkCount),
				attribute.Int("streamkit.entry_count", 1),
				attribute.Int("streamkit.chunk_size_bytes", chunkSize),
				attribute.Int64("streamkit.sequence", int64(e.Sequence)),
			))
		defer span.End()

		return e, nil
	})
}

// wrapSegmentStatusEnumerator wraps a status enumerator to emit chunk spans.
func wrapSegmentStatusEnumerator(ctx context.Context, enum enumerators.Enumerator[*SegmentStatus], parentSpan trace.Span, spanName string) enumerators.Enumerator[*SegmentStatus] {
	tracer := telemetry.GetTracer()
	chunkCount := 0

	return enumerators.Map(enum, func(s *SegmentStatus) (*SegmentStatus, error) {
		chunkCount++

		baseAttrs := []attribute.KeyValue{
			attribute.Int("streamkit.chunk_num", chunkCount),
			attribute.Int64("streamkit.first_timestamp", s.FirstTimestamp),
			attribute.Int64("streamkit.last_timestamp", s.LastTimestamp),
		}
		seqAttrs := telemetry.WithSequenceRange(s.FirstSequence, s.LastSequence)
		allAttrs := append(baseAttrs, seqAttrs...)
		_, span := tracer.Start(ctx, spanName,
			trace.WithAttributes(allAttrs...))
		defer span.End()

		return s, nil
	})
}

// generateOrGetRequestID extracts or generates a request ID for context.
func generateOrGetRequestID(ctx context.Context) uuid.UUID {
	if id, ok := telemetry.RequestIDFromContext(ctx); ok {
		return id
	}
	return uuid.New()
}
