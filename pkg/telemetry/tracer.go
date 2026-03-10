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
// It uses the global TracerProvider on first call. For proper configuration,
// applications should call telemetry.Initialize() (internal package) or
// otel.SetTracerProvider() during startup before using GetTracer.
func GetTracer() trace.Tracer {
	tracerOnce.Do(func() {
		globalTracer = otel.Tracer("github.com/fgrzl/streamkit")
	})
	return globalTracer
}

// InitializeTracer sets the global tracer explicitly and ensures it's only
// initialized once. This should be called during application startup after
// setting up the TracerProvider via otel.SetTracerProvider().
func InitializeTracer(tracer trace.Tracer) {
	tracerOnce.Do(func() {
		globalTracer = tracer
	})
}
