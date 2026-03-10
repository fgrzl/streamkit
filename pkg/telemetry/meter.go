package telemetry

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// MeterName is the name passed to otel.Meter for streamkit instrumentation.
const MeterName = "github.com/fgrzl/streamkit"

// GetMeter returns the global meter instance for streamkit. Use it to create
// counters, histograms, and other instruments. The global MeterProvider must be
// set (e.g. via internal/telemetry.Initialize or otel.SetMeterProvider) for
// metrics to be exported.
func GetMeter() metric.Meter {
	return otel.Meter(MeterName)
}
