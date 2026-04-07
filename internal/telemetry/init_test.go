package telemetry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func restoreGlobalTelemetry(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	prevMP := otel.GetMeterProvider()
	prevProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetMeterProvider(prevMP)
		otel.SetTextMapPropagator(prevProp)
	})
}

func makeTraceID(fill byte) oteltrace.TraceID {
	var traceID oteltrace.TraceID
	for i := range traceID {
		traceID[i] = fill
	}
	return traceID
}

func TestInitializeShouldInstallProvidersAndCompositePropagatorWhenEndpointEmpty(t *testing.T) {
	restoreGlobalTelemetry(t)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	shutdown, err := Initialize(context.Background(), Config{ServiceName: "streamkit-test", ServiceVersion: "1.2.3"})
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	member, err := baggage.NewMember("tenant", "tenant-123")
	require.NoError(t, err)
	bag, err := baggage.New(member)
	require.NoError(t, err)

	ctx := baggage.ContextWithBaggage(context.Background(), bag)
	ctx, span := otel.Tracer("test").Start(ctx, "span")
	defer span.End()

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	assert.NotEmpty(t, carrier.Get("traceparent"))
	assert.Contains(t, carrier.Get("baggage"), "tenant=tenant-123")
	require.NoError(t, shutdown(context.Background()))
}

func TestNewInMemoryTracerProviderShouldExportSpans(t *testing.T) {
	tp, exporter, err := NewInMemoryTracerProvider()
	require.NoError(t, err)
	require.NotNil(t, tp)
	require.NotNil(t, exporter)
	defer tp.Shutdown(context.Background())

	_, span := tp.Tracer("test").Start(context.Background(), "in-memory-span")
	span.End()

	require.NoError(t, tp.ForceFlush(context.Background()))
	spans := exporter.GetSpans()
	require.NotEmpty(t, spans)
	assert.Equal(t, "in-memory-span", spans[0].Name)
}

func TestSamplerFromRatioShouldReturnExpectedDecisionsAndDescriptions(t *testing.T) {
	tests := []struct {
		name         string
		ratio        float64
		expectedDesc string
		lowDecision  sdktrace.SamplingDecision
		highDecision sdktrace.SamplingDecision
	}{
		{
			name:         "default ratio",
			ratio:        0,
			expectedDesc: "ParentBased{root:TraceIDRatioBased{0.1},remoteParentSampled:AlwaysOnSampler,remoteParentNotSampled:AlwaysOffSampler,localParentSampled:AlwaysOnSampler,localParentNotSampled:AlwaysOffSampler}",
			lowDecision:  sdktrace.RecordAndSample,
			highDecision: sdktrace.Drop,
		},
		{
			name:         "fractional ratio",
			ratio:        0.25,
			expectedDesc: "ParentBased{root:TraceIDRatioBased{0.25},remoteParentSampled:AlwaysOnSampler,remoteParentNotSampled:AlwaysOffSampler,localParentSampled:AlwaysOnSampler,localParentNotSampled:AlwaysOffSampler}",
			lowDecision:  sdktrace.RecordAndSample,
			highDecision: sdktrace.Drop,
		},
		{
			name:         "always on ratio",
			ratio:        1,
			expectedDesc: "AlwaysOnSampler",
			lowDecision:  sdktrace.RecordAndSample,
			highDecision: sdktrace.RecordAndSample,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sampler := samplerFromRatio(tt.ratio)

			assert.Equal(t, tt.expectedDesc, sampler.Description())
			assert.Equal(t, tt.lowDecision, sampler.ShouldSample(sdktrace.SamplingParameters{TraceID: makeTraceID(0x00)}).Decision)
			assert.Equal(t, tt.highDecision, sampler.ShouldSample(sdktrace.SamplingParameters{TraceID: makeTraceID(0xFF)}).Decision)
		})
	}
}

func TestNewResourceShouldIncludeConfiguredServiceAttributes(t *testing.T) {
	res, err := newResource("streamkit", "1.2.3")
	require.NoError(t, err)

	attrs := make(map[string]string, len(res.Attributes()))
	for _, attr := range res.Attributes() {
		attrs[string(attr.Key)] = attr.Value.AsString()
	}

	assert.Equal(t, "streamkit", attrs["service.name"])
	assert.Equal(t, "1.2.3", attrs["service.version"])
}

func TestNewResourceShouldOmitServiceVersionWhenEmpty(t *testing.T) {
	res, err := newResource("streamkit", "")
	require.NoError(t, err)

	attrs := make(map[string]string, len(res.Attributes()))
	for _, attr := range res.Attributes() {
		attrs[string(attr.Key)] = attr.Value.AsString()
	}

	assert.Equal(t, "streamkit", attrs["service.name"])
	_, ok := attrs["service.version"]
	assert.False(t, ok)
}
