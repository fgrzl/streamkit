package client

import (
	"context"
	"time"

	"github.com/fgrzl/streamkit/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// NewOTelClientMetrics returns a ClientMetrics implementation that records
// produce/consume latencies and subscription events as OpenTelemetry metrics.
// Use with NewClientWithMetrics(provider, timeout, NewOTelClientMetrics()).
func NewOTelClientMetrics() ClientMetrics {
	meter := telemetry.GetMeter()
	produceLatency, _ := meter.Float64Histogram(
		"streamkit.client.produce.latency",
		metric.WithDescription("Produce operation latency"),
		metric.WithUnit("ms"),
	)
	consumeLatency, _ := meter.Float64Histogram(
		"streamkit.client.consume.latency",
		metric.WithDescription("Consume operation latency"),
		metric.WithUnit("ms"),
	)
	replayTotal, _ := meter.Int64Counter(
		"streamkit.client.subscription.replay.total",
		metric.WithDescription("Subscription replay attempts"),
	)
	replaySuccess, _ := meter.Int64Counter(
		"streamkit.client.subscription.replay.success",
		metric.WithDescription("Subscription replay successes"),
	)
	replayDuration, _ := meter.Float64Histogram(
		"streamkit.client.subscription.replay.duration",
		metric.WithDescription("Subscription replay duration"),
		metric.WithUnit("ms"),
	)
	reconnectQueueDepth, _ := meter.Int64Gauge(
		"streamkit.client.subscription.reconnect.queue.depth",
		metric.WithDescription("Current reconnect dispatcher queue depth"),
		metric.WithUnit("1"),
	)
	reconnectQueueBlockedTotal, _ := meter.Int64Counter(
		"streamkit.client.subscription.reconnect.queue.blocked.total",
		metric.WithDescription("Reconnect requests that had to block because the reconnect queue was full"),
		metric.WithUnit("1"),
	)
	reconnectQueueBlockedDuration, _ := meter.Float64Histogram(
		"streamkit.client.subscription.reconnect.queue.blocked.duration",
		metric.WithDescription("Time spent waiting to enqueue reconnect requests when the reconnect queue was full"),
		metric.WithUnit("ms"),
	)
	reconnectQueueWaitDuration, _ := meter.Float64Histogram(
		"streamkit.client.subscription.reconnect.queue.wait.duration",
		metric.WithDescription("Time reconnect requests spent waiting in the reconnect dispatcher queue before processing"),
		metric.WithUnit("ms"),
	)
	handlerTimeoutTotal, _ := meter.Int64Counter(
		"streamkit.client.subscription.handler.timeout.total",
		metric.WithDescription("Subscription handler timeout count"),
	)
	handlerPanicTotal, _ := meter.Int64Counter(
		"streamkit.client.subscription.handler.panic.total",
		metric.WithDescription("Subscription handler panic count"),
	)
	coalescedTotal, _ := meter.Int64Counter(
		"streamkit.client.subscription.coalesced.total",
		metric.WithDescription("Subscription updates coalesced while handlers were saturated"),
	)
	return &otelClientMetrics{
		produceLatency:                produceLatency,
		consumeLatency:                consumeLatency,
		replayTotal:                   replayTotal,
		replaySuccess:                 replaySuccess,
		replayDuration:                replayDuration,
		reconnectQueueDepth:           reconnectQueueDepth,
		reconnectQueueBlockedTotal:    reconnectQueueBlockedTotal,
		reconnectQueueBlockedDuration: reconnectQueueBlockedDuration,
		reconnectQueueWaitDuration:    reconnectQueueWaitDuration,
		handlerTimeoutTotal:           handlerTimeoutTotal,
		handlerPanicTotal:             handlerPanicTotal,
		coalescedTotal:                coalescedTotal,
	}
}

type otelClientMetrics struct {
	produceLatency                metric.Float64Histogram
	consumeLatency                metric.Float64Histogram
	replayTotal                   metric.Int64Counter
	replaySuccess                 metric.Int64Counter
	replayDuration                metric.Float64Histogram
	reconnectQueueDepth           metric.Int64Gauge
	reconnectQueueBlockedTotal    metric.Int64Counter
	reconnectQueueBlockedDuration metric.Float64Histogram
	reconnectQueueWaitDuration    metric.Float64Histogram
	handlerTimeoutTotal           metric.Int64Counter
	handlerPanicTotal             metric.Int64Counter
	coalescedTotal                metric.Int64Counter
}

func (o *otelClientMetrics) RecordProduceLatency(space, segment string, duration time.Duration) {
	if o.produceLatency != nil {
		o.produceLatency.Record(context.Background(), float64(duration.Milliseconds()),
			metric.WithAttributes(
				attribute.String("streamkit.space", space),
				attribute.String("streamkit.segment", segment),
			))
	}
}

func (o *otelClientMetrics) RecordConsumeLatency(space, segment string, duration time.Duration) {
	if o.consumeLatency != nil {
		o.consumeLatency.Record(context.Background(), float64(duration.Milliseconds()),
			metric.WithAttributes(
				attribute.String("streamkit.space", space),
				attribute.String("streamkit.segment", segment),
			))
	}
}

func (o *otelClientMetrics) RecordSubscriptionReplay(id string, success bool, duration time.Duration) {
	// Low-cardinality attributes only (no subscription_id) for production backends.
	successAttr := attribute.Bool("streamkit.replay.success", success)
	if o.replayTotal != nil {
		o.replayTotal.Add(context.Background(), 1, metric.WithAttributes(successAttr))
	}
	if success && o.replaySuccess != nil {
		o.replaySuccess.Add(context.Background(), 1, metric.WithAttributes(successAttr))
	}
	if o.replayDuration != nil {
		o.replayDuration.Record(context.Background(), float64(duration.Milliseconds()),
			metric.WithAttributes(successAttr))
	}
}

func (o *otelClientMetrics) RecordHandlerTimeout(id string) {
	if o.handlerTimeoutTotal != nil {
		o.handlerTimeoutTotal.Add(context.Background(), 1)
	}
}

func (o *otelClientMetrics) RecordHandlerPanic(id string) {
	if o.handlerPanicTotal != nil {
		o.handlerPanicTotal.Add(context.Background(), 1)
	}
}

func (o *otelClientMetrics) RecordSubscriptionCoalesced(id string) {
	if o.coalescedTotal != nil {
		o.coalescedTotal.Add(context.Background(), 1)
	}
}

func (o *otelClientMetrics) RecordReconnectQueueDepth(depth int) {
	if o.reconnectQueueDepth != nil {
		o.reconnectQueueDepth.Record(context.Background(), int64(depth))
	}
}

func (o *otelClientMetrics) RecordReconnectQueueBlocked(duration time.Duration) {
	if o.reconnectQueueBlockedTotal != nil {
		o.reconnectQueueBlockedTotal.Add(context.Background(), 1)
	}
	if o.reconnectQueueBlockedDuration != nil {
		o.reconnectQueueBlockedDuration.Record(context.Background(), float64(duration.Milliseconds()))
	}
}

func (o *otelClientMetrics) RecordReconnectQueueWait(duration time.Duration) {
	if o.reconnectQueueWaitDuration != nil {
		o.reconnectQueueWaitDuration.Record(context.Background(), float64(duration.Milliseconds()))
	}
}
