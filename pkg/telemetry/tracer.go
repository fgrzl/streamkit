// Package telemetry provides OpenTelemetry instrumentation helpers for streamkit.
package telemetry

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// GetTracer returns the tracer for streamkit from the global TracerProvider.
// It does not cache; each call uses the current otel.TracerProvider, so tests
// and apps can call otel.SetTracerProvider() before any GetTracer() and have
// it take effect. For proper configuration, call telemetry.Initialize()
// (internal package) or otel.SetTracerProvider() during startup.
func GetTracer() trace.Tracer {
	return otel.Tracer("github.com/fgrzl/streamkit")
}
