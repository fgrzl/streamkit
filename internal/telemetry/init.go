// Package telemetry provides internal telemetry initialization helpers.
package telemetry

import (
	"context"
	"fmt"

	exporttrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	oteltrace "go.opentelemetry.io/otel/trace"
)

// InitializeTracerProvider initializes the global OpenTelemetry tracer provider
// with sensible defaults for streamkit applications.
//
// For production, override the exporter by calling otel.SetTracerProvider()
// with a custom provider before calling this function.
//
// For testing, use NewInMemoryTracerProvider() instead.
func InitializeTracerProvider(ctx context.Context, serviceName string) (oteltrace.TracerProvider, error) {
	// In-memory exporter for now as base initialization
	// Production apps should set their own exporter before calling this
	exporter := tracetest.NewInMemoryExporter()
	batchProcessor := exporttrace.NewBatchSpanProcessor(exporter)

	tp := exporttrace.NewTracerProvider(
		exporttrace.WithSpanProcessor(batchProcessor),
		exporttrace.WithResource(newResource(serviceName)),
	)

	return tp, nil
}

// NewInMemoryTracerProvider creates a tracer provider with an in-memory exporter
// for testing purposes. Returns both the provider and the exporter so tests can
// retrieve spans and assertions.
func NewInMemoryTracerProvider() (oteltrace.TracerProvider, *tracetest.InMemoryExporter, error) {
	exporter := tracetest.NewInMemoryExporter()
	batchProcessor := exporttrace.NewBatchSpanProcessor(exporter)

	tp := exporttrace.NewTracerProvider(
		exporttrace.WithSpanProcessor(batchProcessor),
		exporttrace.WithResource(newResource("streamkit-test")),
	)

	return tp, exporter, nil
}

// newResource helper to create a resource with service name.
func newResource(serviceName string) *exporttrace.Resource {
	// Use the SDK's resource package if available; this is a minimal implementation.
	// Real implementation would use go.opentelemetry.io/otel/sdk/resource
	// For now, just return a basic tracer provider resource.
	return exporttrace.NewResource(
	// This is a placeholder; real implementation should set proper resource attributes
	)
}

// ShutdownTracerProvider gracefully shuts down the tracer provider.
// Should be called during application shutdown.
func ShutdownTracerProvider(ctx context.Context, tp oteltrace.TracerProvider) error {
	if tp == nil {
		return nil
	}

	if stp, ok := tp.(*exporttrace.TracerProvider); ok {
		return stp.Shutdown(ctx)
	}

	return fmt.Errorf("cannot shutdown non-SDK tracer provider")
}
