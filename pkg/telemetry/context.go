package telemetry

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

// Typed context keys to avoid collisions with other packages.
type contextKey string

const (
	traceIDKey   contextKey = "streamkit:trace_id"
	requestIDKey contextKey = "streamkit:request_id"
)

// RequestIDFromContext retrieves the request ID from the context, if present.
// Returns (uuid.Nil, false) if not found.
func RequestIDFromContext(ctx context.Context) (uuid.UUID, bool) {
	id, ok := ctx.Value(requestIDKey).(uuid.UUID)
	return id, ok
}

// WithTraceID returns a new context with the trace ID added.
// This is typically called internally during span creation for log correlation.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey, traceID)
}

// TraceIDFromContext retrieves the trace ID from the context, if present.
// If not explicitly set, attempts to extract from the active span's trace ID.
func TraceIDFromContext(ctx context.Context) string {
	// Check if explicitly set
	if id, ok := ctx.Value(traceIDKey).(string); ok {
		return id
	}

	// Try to extract from active span
	span := trace.SpanFromContext(ctx)
	if span != nil && span.SpanContext().IsValid() {
		return span.SpanContext().TraceID().String()
	}

	return ""
}

// AddTraceContextToLogger returns a new slog value slice with trace context attributes.
// Used to add trace_id and span_id to slog records for correlation.
// Returns empty slice if span context is not valid.
func AddTraceContextToLogger(ctx context.Context) []any {
	span := trace.SpanFromContext(ctx)
	if span == nil || !span.SpanContext().IsValid() {
		return []any{}
	}

	return []any{
		slog.String("trace_id", span.SpanContext().TraceID().String()),
		slog.String("span_id", span.SpanContext().SpanID().String()),
	}
}
