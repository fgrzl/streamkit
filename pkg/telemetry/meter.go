package telemetry

import (
	"context"
	"time"

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

// WSKitQueueMetrics records websocket muxer queue pressure signals.
type WSKitQueueMetrics struct {
	writeQueueDepth       metric.Int64Gauge
	writeQueueBlocked     metric.Int64Counter
	writeQueueBlockTime   metric.Float64Histogram
	writeQueueFallbacks   metric.Int64Counter
	writeQueueSaturations metric.Int64Counter
}

// NewWSKitQueueMetrics returns queue pressure instruments for websocket muxers.
func NewWSKitQueueMetrics() *WSKitQueueMetrics {
	meter := GetMeter()
	writeQueueDepth, _ := meter.Int64Gauge(
		"streamkit.transport.wskit.write.queue.depth",
		metric.WithDescription("Current websocket muxer write queue depth"),
		metric.WithUnit("1"),
	)
	writeQueueBlocked, _ := meter.Int64Counter(
		"streamkit.transport.wskit.write.queue.blocked.total",
		metric.WithDescription("Number of websocket muxer writes that had to wait for queue capacity"),
		metric.WithUnit("1"),
	)
	writeQueueBlockTime, _ := meter.Float64Histogram(
		"streamkit.transport.wskit.write.queue.blocked.duration",
		metric.WithDescription("Time spent waiting for websocket muxer queue capacity"),
		metric.WithUnit("ms"),
	)
	writeQueueFallbacks, _ := meter.Int64Counter(
		"streamkit.transport.wskit.write.queue.fallback.total",
		metric.WithDescription("Number of websocket muxer writes that fell back to synchronous send because the queue was full"),
		metric.WithUnit("1"),
	)
	writeQueueSaturations, _ := meter.Int64Counter(
		"streamkit.transport.wskit.write.queue.saturation.total",
		metric.WithDescription("Number of sustained websocket muxer queue saturation events"),
		metric.WithUnit("1"),
	)
	return &WSKitQueueMetrics{
		writeQueueDepth:       writeQueueDepth,
		writeQueueBlocked:     writeQueueBlocked,
		writeQueueBlockTime:   writeQueueBlockTime,
		writeQueueFallbacks:   writeQueueFallbacks,
		writeQueueSaturations: writeQueueSaturations,
	}
}

// RecordWriteQueueDepth records the current websocket muxer queue depth.
func (m *WSKitQueueMetrics) RecordWriteQueueDepth(ctx context.Context, role string, depth int64) {
	if m == nil || m.writeQueueDepth == nil {
		return
	}
	m.writeQueueDepth.Record(metricContext(ctx), depth, metric.WithAttributes(
		WithTransportType("websocket"),
		WithMuxerRole(role),
	))
}

// RecordWriteQueueBlocked increments the blocked-write counter and duration histogram.
func (m *WSKitQueueMetrics) RecordWriteQueueBlocked(ctx context.Context, role string, duration time.Duration) {
	if m == nil {
		return
	}
	if m.writeQueueBlocked != nil {
		m.writeQueueBlocked.Add(metricContext(ctx), 1, metric.WithAttributes(
			WithTransportType("websocket"),
			WithMuxerRole(role),
		))
	}
	if m.writeQueueBlockTime != nil {
		m.writeQueueBlockTime.Record(metricContext(ctx), float64(duration.Milliseconds()), metric.WithAttributes(
			WithTransportType("websocket"),
			WithMuxerRole(role),
		))
	}
}

// RecordWriteQueueFallback increments the websocket queue fallback counter.
func (m *WSKitQueueMetrics) RecordWriteQueueFallback(ctx context.Context, role string) {
	if m == nil || m.writeQueueFallbacks == nil {
		return
	}
	m.writeQueueFallbacks.Add(metricContext(ctx), 1, metric.WithAttributes(
		WithTransportType("websocket"),
		WithMuxerRole(role),
	))
}

// RecordWriteQueueSaturation increments the sustained saturation counter.
func (m *WSKitQueueMetrics) RecordWriteQueueSaturation(ctx context.Context, role string) {
	if m == nil || m.writeQueueSaturations == nil {
		return
	}
	m.writeQueueSaturations.Add(metricContext(ctx), 1, metric.WithAttributes(
		WithTransportType("websocket"),
		WithMuxerRole(role),
	))
}

func metricContext(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	return context.Background()
}
