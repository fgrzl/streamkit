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
	handlerTimeoutTotal, _ := meter.Int64Counter(
		"streamkit.client.subscription.handler.timeout.total",
		metric.WithDescription("Subscription handler timeout count"),
	)
	handlerPanicTotal, _ := meter.Int64Counter(
		"streamkit.client.subscription.handler.panic.total",
		metric.WithDescription("Subscription handler panic count"),
	)
	return &otelClientMetrics{
		produceLatency:      produceLatency,
		consumeLatency:      consumeLatency,
		replayTotal:         replayTotal,
		replaySuccess:       replaySuccess,
		handlerTimeoutTotal: handlerTimeoutTotal,
		handlerPanicTotal:   handlerPanicTotal,
	}
}

type otelClientMetrics struct {
	produceLatency      metric.Float64Histogram
	consumeLatency      metric.Float64Histogram
	replayTotal         metric.Int64Counter
	replaySuccess       metric.Int64Counter
	handlerTimeoutTotal metric.Int64Counter
	handlerPanicTotal   metric.Int64Counter
}

func (o *otelClientMetrics) RecordProduceLatency(space, segment string, duration time.Duration) {
	if o.produceLatency != nil {
		o.produceLatency.Record(context.Background(), duration.Seconds()*1000,
			metric.WithAttributes(
				attribute.String("streamkit.space", space),
				attribute.String("streamkit.segment", segment),
			))
	}
}

func (o *otelClientMetrics) RecordConsumeLatency(space, segment string, duration time.Duration) {
	if o.consumeLatency != nil {
		o.consumeLatency.Record(context.Background(), duration.Seconds()*1000,
			metric.WithAttributes(
				attribute.String("streamkit.space", space),
				attribute.String("streamkit.segment", segment),
			))
	}
}

func (o *otelClientMetrics) RecordSubscriptionReplay(id string, success bool, duration time.Duration) {
	attrs := metric.WithAttributes(attribute.String("streamkit.subscription_id", id))
	if o.replayTotal != nil {
		o.replayTotal.Add(context.Background(), 1, attrs)
	}
	if success && o.replaySuccess != nil {
		o.replaySuccess.Add(context.Background(), 1, attrs)
	}
}

func (o *otelClientMetrics) RecordHandlerTimeout(id string) {
	if o.handlerTimeoutTotal != nil {
		o.handlerTimeoutTotal.Add(context.Background(), 1, metric.WithAttributes(attribute.String("streamkit.subscription_id", id)))
	}
}

func (o *otelClientMetrics) RecordHandlerPanic(id string) {
	if o.handlerPanicTotal != nil {
		o.handlerPanicTotal.Add(context.Background(), 1, metric.WithAttributes(attribute.String("streamkit.subscription_id", id)))
	}
}
