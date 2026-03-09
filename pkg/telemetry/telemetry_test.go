package telemetry_test

import (
	"context"
	"testing"

	"github.com/fgrzl/streamkit/pkg/telemetry"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestGetTracerReturnsGlobalTracer(t *testing.T) {
	// Arrange
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(
		trace.WithSpanProcessor(trace.NewSimpleSpanProcessor(exporter)),
	)
	otel.SetTracerProvider(tp)
	defer tp.Shutdown(context.Background())

	// Act
	tracer1 := telemetry.GetTracer()
	tracer2 := telemetry.GetTracer()

	// Assert
	require.NotNil(t, tracer1)
	require.Equal(t, tracer1, tracer2, "GetTracer should return the same instance")
}

func TestRequestIDFromContextReturnsFalseWhenNotPresent(t *testing.T) {
	// Arrange
	ctx := context.Background()

	// Act
	_, ok := telemetry.RequestIDFromContext(ctx)

	// Assert
	assert.False(t, ok)
}

func TestTraceIDFromContextExtractsFromSpan(t *testing.T) {
	// Arrange
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(
		trace.WithSpanProcessor(trace.NewSimpleSpanProcessor(exporter)),
	)
	defer tp.Shutdown(context.Background())

	tracer := tp.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "test_span")
	defer span.End()

	// Act
	traceID := telemetry.TraceIDFromContext(ctx)

	// Assert
	assert.NotEmpty(t, traceID)
	assert.Equal(t, span.SpanContext().TraceID().String(), traceID)
}

func TestAttributeBuilders(t *testing.T) {
	// Test that all attribute builder functions work without panicking

	// Act & Assert
	assert.NotPanics(t, func() { telemetry.WithStoreID(uuid.New()) })
	assert.NotPanics(t, func() { telemetry.WithSpace("test-space") })
	assert.NotPanics(t, func() { telemetry.WithSegment("test-segment") })
	assert.NotPanics(t, func() { telemetry.WithRecordCount(42) })
	assert.NotPanics(t, func() { telemetry.WithBatchSize(10) })
	assert.NotPanics(t, func() { telemetry.WithTransportType("websocket") })
	assert.NotPanics(t, func() { telemetry.WithBackendType("azure") })
}

func TestAddTraceContextToLogger(t *testing.T) {
	// Arrange
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(
		trace.WithSpanProcessor(trace.NewSimpleSpanProcessor(exporter)),
	)
	defer tp.Shutdown(context.Background())

	tracer := tp.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "test_span")
	defer span.End()

	// Act
	attrs := telemetry.AddTraceContextToLogger(ctx)

	// Assert
	assert.Len(t, attrs, 2) // trace_id and span_id
}

func TestAddTraceContextToLoggerWithoutSpan(t *testing.T) {
	// Arrange
	ctx := context.Background()

	// Act
	attrs := telemetry.AddTraceContextToLogger(ctx)

	// Assert
	assert.Empty(t, attrs)
}
