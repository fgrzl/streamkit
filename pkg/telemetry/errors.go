package telemetry

import (
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// RecordError records an error in the span and sets the span status to ERROR.
// This is a convenience helper that calls both span.RecordError() and span.SetStatus().
func RecordError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// RecordErrorMsg records an error in the span with a custom message and sets status to ERROR.
// This is useful when the error message needs to be augmented or when the error
// itself should not be recorded as an exception event.
func RecordErrorMsg(span trace.Span, err error, msg string) {
	if err == nil {
		span.SetStatus(codes.Ok, "")
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, msg)
}

// SetSpanStatusOK marks the span as completed successfully.
func SetSpanStatusOK(span trace.Span) {
	span.SetStatus(codes.Ok, "")
}

// AddEvent adds an event to the span with optional attributes.
// Convenience wrapper around span.AddEvent.
func AddEvent(span trace.Span, name string, opts ...trace.EventOption) {
	span.AddEvent(name, opts...)
}
