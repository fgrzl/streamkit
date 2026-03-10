// Package telemetry provides internal telemetry initialization helpers.
package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Config holds options for initializing the OpenTelemetry SDK.
type Config struct {
	// ServiceName is the name of the service for resource attributes.
	ServiceName string
	// ServiceVersion is optional version (e.g. build or git revision).
	ServiceVersion string
	// OTLPEndpoint is the OTLP endpoint (e.g. http://localhost:4318).
	// If empty, trace and metric exporters are not created and no-op providers are used.
	OTLPEndpoint string
	// Insecure, when true, disables TLS for the OTLP endpoint.
	Insecure bool
}

// ShutdownFunc shuts down tracer and meter providers. Safe to call multiple times.
type ShutdownFunc func(ctx context.Context) error

// Initialize sets up the global OpenTelemetry tracer provider, meter provider,
// and text map propagator with sensible defaults. Returns a shutdown function
// that should be called during application shutdown.
//
// If Config.OTLPEndpoint is empty, a no-op tracer provider and meter provider
// are set; trace and metric data will not be exported. For testing with
// in-memory spans, use NewInMemoryTracerProvider instead and set the returned
// provider via otel.SetTracerProvider before calling GetTracer.
//
// For log-to-trace correlation, wrap the application's slog handler with
// pkg/telemetry.NewTraceContextHandler so that trace_id and span_id are added to log records.
func Initialize(ctx context.Context, cfg Config) (ShutdownFunc, error) {
	res, err := newResource(cfg.ServiceName, cfg.ServiceVersion)
	if err != nil {
		return nil, fmt.Errorf("telemetry resource: %w", err)
	}

	// Composite propagator: W3C Trace Context + Baggage
	prop := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
	otel.SetTextMapPropagator(prop)

	var shutdowns []func(context.Context) error

	if cfg.OTLPEndpoint != "" {
		// OTLP trace exporter
		traceOpts := []otlptracehttp.Option{
			otlptracehttp.WithEndpoint(cfg.OTLPEndpoint),
		}
		if cfg.Insecure {
			traceOpts = append(traceOpts, otlptracehttp.WithInsecure())
		}
		traceExp, err := otlptracehttp.New(ctx, traceOpts...)
		if err != nil {
			return nil, fmt.Errorf("otlp trace exporter: %w", err)
		}
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			sdktrace.WithBatcher(traceExp),
		)
		otel.SetTracerProvider(tp)
		shutdowns = append(shutdowns, func(c context.Context) error { return tp.Shutdown(c) })
	} else {
		// No endpoint: use no-op tracer provider so GetTracer still works
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
		)
		otel.SetTracerProvider(tp)
		shutdowns = append(shutdowns, func(c context.Context) error { return tp.Shutdown(c) })
	}

	if cfg.OTLPEndpoint != "" {
		// OTLP metric exporter
		metricOpts := []otlpmetrichttp.Option{
			otlpmetrichttp.WithEndpoint(cfg.OTLPEndpoint),
		}
		if cfg.Insecure {
			metricOpts = append(metricOpts, otlpmetrichttp.WithInsecure())
		}
		metricExp, err := otlpmetrichttp.New(ctx, metricOpts...)
		if err != nil {
			return nil, fmt.Errorf("otlp metric exporter: %w", err)
		}
		mp := metric.NewMeterProvider(
			metric.WithResource(res),
			metric.WithReader(metric.NewPeriodicReader(metricExp)),
		)
		otel.SetMeterProvider(mp)
		shutdowns = append(shutdowns, func(c context.Context) error { return mp.Shutdown(c) })
	} else {
		// No endpoint: use default meter provider (no-op)
		mp := metric.NewMeterProvider(metric.WithResource(res))
		otel.SetMeterProvider(mp)
		shutdowns = append(shutdowns, func(c context.Context) error { return mp.Shutdown(c) })
	}

	fn := func(ctx context.Context) error {
		var errs []error
		for _, shutdown := range shutdowns {
			if err := shutdown(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		if len(errs) > 0 {
			return fmt.Errorf("telemetry shutdown: %v", errs)
		}
		return nil
	}
	return fn, nil
}

// NewInMemoryTracerProvider creates a tracer provider with an in-memory exporter
// for testing. Returns the provider and the exporter so tests can assert on spans.
// Callers should set the provider globally with otel.SetTracerProvider(tp) before
// using GetTracer, and call tp.Shutdown(ctx) when done.
func NewInMemoryTracerProvider() (*sdktrace.TracerProvider, *tracetest.InMemoryExporter, error) {
	exporter := tracetest.NewInMemoryExporter()
	res, _ := newResource("streamkit-test", "")
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exporter)),
		sdktrace.WithResource(res),
	)
	return tp, exporter, nil
}

// newResource builds a resource with service name and optional version.
func newResource(serviceName, serviceVersion string) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{
		attribute.String("service.name", serviceName),
	}
	if serviceVersion != "" {
		attrs = append(attrs, attribute.String("service.version", serviceVersion))
	}
	return resource.New(context.Background(), resource.WithAttributes(attrs...))
}
