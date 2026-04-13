package test

import (
	"context"
	"testing"

	"github.com/fgrzl/enumerators"
	"github.com/fgrzl/streamkit/pkg/client"
	"github.com/fgrzl/streamkit/pkg/server"
	"github.com/fgrzl/streamkit/pkg/storage/pebblekit"
	"github.com/fgrzl/streamkit/pkg/transport/inprockit"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestShouldOtelTraceContinuityOverWskit(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})

	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(trace.NewSimpleSpanProcessor(exporter)))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	defer tp.Shutdown(context.Background())

	harness := pebblekitTestHarness(t)
	storeID := uuid.New()
	ctx := t.Context()
	space, segment := "space0", "segment0"

	_, err := harness.Client.Peek(ctx, storeID, space, segment)
	require.NoError(t, err)

	require.NoError(t, tp.ForceFlush(context.Background()))
	spans := exporter.GetSpans()
	require.NotEmpty(t, spans, "expected at least one span")

	var transportSpan, serverSpan *tracetest.SpanStub
	for i := range spans {
		s := &spans[i]
		switch s.Name {
		case "streamkit.transport.call_stream":
			transportSpan = s
		case "streamkit.server.peek":
			serverSpan = s
		}
	}
	require.NotNil(t, transportSpan, "expected streamkit.transport.call_stream span")
	require.NotNil(t, serverSpan, "expected streamkit.server.peek span")
	assert.Equal(t, transportSpan.SpanContext.TraceID(), serverSpan.SpanContext.TraceID(),
		"client and server spans should share the same trace ID")
}

func TestShouldOtelNoEnumeratorChunkSpans(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})

	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(trace.NewSimpleSpanProcessor(exporter)))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	defer tp.Shutdown(context.Background())

	options := &pebblekit.PebbleStoreOptions{Path: t.TempDir()}
	factory, err := pebblekit.NewStoreFactory(options)
	require.NoError(t, err)
	nodeManager := server.NewNodeManager(server.WithStoreFactory(factory))
	provider := inprockit.NewInProcBidiStreamProvider(t.Context(), nodeManager)
	tracingClient := client.NewClientWithTracing(provider)
	t.Cleanup(func() {
		tracingClient.Close()
		nodeManager.Close()
	})

	ctx := t.Context()
	storeID := uuid.New()
	enum := tracingClient.GetSpaces(ctx, storeID)
	for enum.MoveNext() {
		_, _ = enum.Current()
	}
	enum.Dispose()

	require.NoError(t, tp.ForceFlush(context.Background()))
	spans := exporter.GetSpans()
	for i := range spans {
		assert.NotEqual(t, "streamkit.client.enumerator_chunk", spans[i].Name,
			"per-item enumerator_chunk spans should be pruned")
	}
}

func TestShouldOtelConsumeTraceContinuityOverInproc(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})

	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(trace.NewSimpleSpanProcessor(exporter)))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	defer tp.Shutdown(context.Background())

	options := &pebblekit.PebbleStoreOptions{Path: t.TempDir()}
	factory, err := pebblekit.NewStoreFactory(options)
	require.NoError(t, err)

	nodeManager := server.NewNodeManager(server.WithStoreFactory(factory))
	provider := inprockit.NewInProcBidiStreamProvider(t.Context(), nodeManager)
	baseClient := client.NewClient(provider)
	tracingClient := client.NewClientWithTracing(provider)
	t.Cleanup(func() {
		tracingClient.Close()
		baseClient.Close()
		nodeManager.Close()
	})

	ctx := t.Context()
	storeID := uuid.New()
	statuses, err := enumerators.ToSlice(baseClient.Produce(ctx, storeID, "space0", "segment0", generateRange(0, 1)))
	require.NoError(t, err)
	require.NotEmpty(t, statuses)

	enum := tracingClient.ConsumeSegment(ctx, storeID, &client.ConsumeSegment{Space: "space0", Segment: "segment0", MinSequence: 1})
	entries, err := enumerators.ToSlice(enum)
	enum.Dispose()
	require.NoError(t, err)
	require.NotEmpty(t, entries)

	require.NoError(t, tp.ForceFlush(context.Background()))
	spans := exporter.GetSpans()

	var clientSpan, serverSpan *tracetest.SpanStub
	for i := range spans {
		s := &spans[i]
		switch s.Name {
		case "streamkit.client.consume_segment":
			clientSpan = s
		case "streamkit.server.consume_segment":
			serverSpan = s
		}
	}

	require.NotNil(t, clientSpan, "expected streamkit.client.consume_segment span")
	require.NotNil(t, serverSpan, "expected streamkit.server.consume_segment span")
	assert.Equal(t, clientSpan.SpanContext.TraceID(), serverSpan.SpanContext.TraceID(),
		"client and server spans should share the same trace ID for detached consume contexts")
}
