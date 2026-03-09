// Package telemetry provides OpenTelemetry instrumentation helpers for streamkit.
package telemetry

import (
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

var (
	globalTracer trace.Tracer
	tracerOnce   sync.Once
)

// GetTracer returns the global tracer instance for streamkit.
// It initializes a default tracer on first call if InitializeTracerProvider
// was not called explicitly. For proper configuration, applications should call
// InitializeTracerProvider() during startup before using GetTracer.
func GetTracer() trace.Tracer {
	tracerOnce.Do(func() {
		globalTracer = otel.Tracer("github.com/fgrzl/streamkit")
	})
	return globalTracer
}

// InitializeTracer sets the global tracer explicitly and ensures it's only
// initialized once. This should be called during application startup after
// setting up the TracerProvider via otel.SetTracerProvider().
// Use InitializeTracerProvider() for a complete setup with sensible defaults.
func InitializeTracer(tracer trace.Tracer) {
	tracerOnce.Do(func() {
		globalTracer = tracer
	})
}
