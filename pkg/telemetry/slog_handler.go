package telemetry

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// TraceContextHandler is a log handler that enriches log records with trace context.
// It wraps an existing slog.Handler and adds trace_id and span_id attributes
// extracted from the span context, enabling correlation between logs and traces.
type TraceContextHandler struct {
	next slog.Handler
}

// NewTraceContextHandler wraps an existing handler with trace context enrichment.
// The returned handler will add trace_id and span_id attributes to all log records
// that are part of an active trace span.
//
// Wire this into your application during bootstrap so that logs correlate with
// traces. Example after initializing OTel (e.g. internal/telemetry.Initialize):
//
//	h := slog.NewJSONHandler(os.Stdout, nil)
//	slog.SetDefault(slog.New(telemetry.NewTraceContextHandler(h)))
func NewTraceContextHandler(next slog.Handler) *TraceContextHandler {
	return &TraceContextHandler{next: next}
}

// Enabled implements slog.Handler.
// Delegates to the wrapped handler.
func (h *TraceContextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

// Handle implements slog.Handler.
// It enriches the record with trace context before passing to the wrapped handler.
func (h *TraceContextHandler) Handle(ctx context.Context, record slog.Record) error {
	// Extract trace context from the span
	span := trace.SpanFromContext(ctx)
	if span != nil && span.SpanContext().IsValid() {
		spanCtx := span.SpanContext()
		record.AddAttrs(
			slog.String("trace_id", spanCtx.TraceID().String()),
			slog.String("span_id", spanCtx.SpanID().String()),
		)
		// Add a flag indicating this record is traced
		if span.IsRecording() {
			record.AddAttrs(slog.Bool("traced", true))
		}
	}

	return h.next.Handle(ctx, record)
}

// WithAttrs implements slog.Handler.
// Returns a new handler with the attributes added.
func (h *TraceContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &TraceContextHandler{next: h.next.WithAttrs(attrs)}
}

// WithGroup implements slog.Handler.
// Returns a new handler with the group added.
func (h *TraceContextHandler) WithGroup(name string) slog.Handler {
	return &TraceContextHandler{next: h.next.WithGroup(name)}
}
