package telemetry_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/fgrzl/streamkit/pkg/telemetry"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type captureHandler struct {
	record slog.Record
	seen   bool
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *captureHandler) Handle(_ context.Context, record slog.Record) error {
	h.record = record.Clone()
	h.seen = true
	return nil
}

func (h *captureHandler) WithAttrs(_ []slog.Attr) slog.Handler {
	return h
}

func (h *captureHandler) WithGroup(_ string) slog.Handler {
	return h
}

func recordAttrs(record slog.Record) map[string]string {
	attrs := make(map[string]string)
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.String()
		return true
	})
	return attrs
}

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

func TestAddTraceContextToLoggerIncludesRequestIDWithoutSpan(t *testing.T) {
	// Arrange
	id := uuid.New()
	ctx := telemetry.WithRequestIDContext(context.Background(), id)

	// Act
	attrs := telemetry.AddTraceContextToLogger(ctx)

	// Assert
	require.Len(t, attrs, 1)
	requestIDAttr, ok := attrs[0].(slog.Attr)
	require.True(t, ok)
	assert.Equal(t, "request_id", requestIDAttr.Key)
	assert.Equal(t, id.String(), requestIDAttr.Value.String())
}

func TestWithRequestIDContextRoundtrip(t *testing.T) {
	// Arrange
	ctx := context.Background()
	id := uuid.New()

	// Act
	ctx = telemetry.WithRequestIDContext(ctx, id)
	got, ok := telemetry.RequestIDFromContext(ctx)

	// Assert
	assert.True(t, ok)
	assert.Equal(t, id, got)
}

func TestTraceContextHandlerAddsTraceAndRequestID(t *testing.T) {
	// Arrange
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(
		trace.WithSpanProcessor(trace.NewSimpleSpanProcessor(exporter)),
	)
	defer tp.Shutdown(context.Background())

	requestID := uuid.New()
	tracer := tp.Tracer("test")
	ctx := telemetry.WithRequestIDContext(context.Background(), requestID)
	ctx, span := tracer.Start(ctx, "test_span")
	defer span.End()

	sink := &captureHandler{}
	logger := slog.New(telemetry.NewTraceContextHandler(sink))

	// Act
	logger.InfoContext(ctx, "test")

	// Assert
	require.True(t, sink.seen)
	attrs := recordAttrs(sink.record)
	assert.Equal(t, requestID.String(), attrs["request_id"])
	assert.Equal(t, span.SpanContext().TraceID().String(), attrs["trace_id"])
	assert.Equal(t, span.SpanContext().SpanID().String(), attrs["span_id"])
	assert.Equal(t, "true", attrs["traced"])
}

func TestGetMeterReturnsNonNil(t *testing.T) {
	// Act
	meter := telemetry.GetMeter()

	// Assert
	require.NotNil(t, meter)
}
