package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

var (
	promReaderFactory = prometheusComponents
	otlpReaderFactory = buildOTLPReader
	instrumentFactory = newOtelInstruments
)

// TelemetryConfig controls how metrics are exported.
type TelemetryConfig struct {
	Enabled      bool
	Port         string
	ServiceName  string
	OtlpEndpoint string
	OtlpInsecure bool
}

// Setup configures OpenTelemetry metrics with a Prometheus exporter and optional OTLP exporter.
// It returns a Recorder, the Prometheus HTTP handler, and a shutdown function.
func Setup(ctx context.Context, cfg TelemetryConfig) (*Recorder, http.Handler, func(context.Context) error, error) {
	if !cfg.Enabled {
		return NewRecorder(), nil, func(context.Context) error { return nil }, nil
	}

	if cfg.ServiceName == "" {
		cfg.ServiceName = "nba-data-service"
	}

	promReader, promHandler, err := promReaderFactory()
	if err != nil {
		return nil, nil, nil, err
	}

	opts := []sdkmetric.Option{sdkmetric.WithReader(promReader)}

	if cfg.OtlpEndpoint != "" {
		otlpReader, err := otlpReaderFactory(ctx, cfg.OtlpEndpoint, cfg.OtlpInsecure)
		if err != nil {
			return nil, nil, nil, err
		}
		opts = append(opts, sdkmetric.WithReader(otlpReader))
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(cfg.ServiceName)),
	)
	if err != nil {
		return nil, nil, nil, err //nolint:wrapcheck // error text is logged and shown to callers as is; wrapping changes it
	}

	opts = append(opts, sdkmetric.WithResource(res))

	provider := sdkmetric.NewMeterProvider(opts...)

	otelInst, err := instrumentFactory(provider)
	if err != nil {
		return nil, nil, nil, err
	}

	rec := newRecorder(otelInst)
	shutdown := func(c context.Context) error {
		return provider.Shutdown(c)
	}

	return rec, promHandler, shutdown, nil
}

func buildOTLPReader(ctx context.Context, endpoint string, insecure bool) (sdkmetric.Reader, error) {
	otlpOpts := []otlpmetrichttp.Option{otlpmetrichttp.WithEndpoint(endpoint)}
	if insecure {
		otlpOpts = append(otlpOpts, otlpmetrichttp.WithInsecure())
	}
	otlpExp, err := otlpmetrichttp.New(ctx, otlpOpts...)
	if err != nil {
		return nil, err //nolint:wrapcheck // error text is logged and shown to callers as is; wrapping changes it
	}
	return sdkmetric.NewPeriodicReader(otlpExp, sdkmetric.WithInterval(15*time.Second)), nil
}

type otelInstruments struct {
	ctx               context.Context
	meter             metric.Meter
	requests          metric.Int64Counter
	requestLatencyMs  metric.Float64Histogram
	providerAttempts  metric.Int64Counter
	providerErrors    metric.Int64Counter
	providerLatencyMs metric.Float64Histogram
	rateLimitHits     metric.Int64Counter
	retryAfterMs      metric.Float64Histogram
	pollerCycles      metric.Int64Counter
	pollerErrors      metric.Int64Counter
	pollerLatencyMs   metric.Float64Histogram
}

func prometheusComponents() (sdkmetric.Reader, http.Handler, error) {
	reg := prometheus.NewRegistry()
	promExp, err := promexporter.New(promexporter.WithRegisterer(reg))
	if err != nil {
		return nil, nil, err //nolint:wrapcheck // error text is logged and shown to callers as is; wrapping changes it
	}
	return promExp, promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), nil
}

// instrumentBuilder creates instruments and keeps the first error, so later calls do nothing after a failure.
type instrumentBuilder struct {
	meter metric.Meter
	err   error
}

func (b *instrumentBuilder) counter(name string) metric.Int64Counter {
	if b.err != nil {
		return nil
	}
	c, err := b.meter.Int64Counter(name)
	b.err = err
	return c
}

func (b *instrumentBuilder) histogram(name string) metric.Float64Histogram {
	if b.err != nil {
		return nil
	}
	h, err := b.meter.Float64Histogram(name)
	b.err = err
	return h
}

func newOtelInstruments(provider metric.MeterProvider) (*otelInstruments, error) {
	meter := provider.Meter("nba-data-service")
	b := &instrumentBuilder{meter: meter}

	inst := &otelInstruments{
		ctx:               context.Background(),
		meter:             meter,
		requests:          b.counter("http_requests_total"),
		requestLatencyMs:  b.histogram("http_request_duration_ms"),
		providerAttempts:  b.counter("provider_attempts_total"),
		providerErrors:    b.counter("provider_errors_total"),
		providerLatencyMs: b.histogram("provider_duration_ms"),
		rateLimitHits:     b.counter("provider_rate_limit_hits_total"),
		retryAfterMs:      b.histogram("provider_retry_after_ms"),
		pollerCycles:      b.counter("poller_cycles_total"),
		pollerErrors:      b.counter("poller_errors_total"),
		pollerLatencyMs:   b.histogram("poller_cycle_duration_ms"),
	}
	if b.err != nil {
		return nil, b.err
	}
	return inst, nil
}

func (o *otelInstruments) recordHTTPRequest(method, path string, status int, duration time.Duration) {
	if o == nil {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String(AttrMethod, method),
		attribute.String(AttrPath, path),
		attribute.Int(AttrStatus, status),
	}
	o.recordCounter(o.requests, 1, attrs...)
	o.recordHistogram(o.requestLatencyMs, float64(duration.Milliseconds()), attrs...)
}

func (o *otelInstruments) recordProviderAttempt(provider string, duration time.Duration, err error) {
	if o == nil {
		return
	}
	attrs := []attribute.KeyValue{attribute.String(AttrProvider, provider)}
	o.recordCounter(o.providerAttempts, 1, attrs...)
	o.recordHistogram(o.providerLatencyMs, float64(duration.Milliseconds()), attrs...)
	if err != nil {
		o.recordCounter(o.providerErrors, 1, attrs...)
	}
}

func (o *otelInstruments) recordRateLimit(provider string, retryAfter time.Duration) {
	if o == nil {
		return
	}
	attrs := []attribute.KeyValue{attribute.String(AttrProvider, provider)}
	o.recordCounter(o.rateLimitHits, 1, attrs...)
	if retryAfter > 0 {
		o.recordHistogram(o.retryAfterMs, float64(retryAfter.Milliseconds()), attrs...)
	}
}

func (o *otelInstruments) recordPoller(duration time.Duration, err error) {
	if o == nil {
		return
	}
	o.recordCounter(o.pollerCycles, 1)
	o.recordHistogram(o.pollerLatencyMs, float64(duration.Milliseconds()))
	if err != nil {
		o.recordCounter(o.pollerErrors, 1)
	}
}

func (o *otelInstruments) recordCounter(counter metric.Int64Counter, value int64, attrs ...attribute.KeyValue) {
	if o == nil {
		return
	}
	counter.Add(o.ctx, value, metric.WithAttributes(attrs...))
}

func (o *otelInstruments) recordHistogram(hist metric.Float64Histogram, value float64, attrs ...attribute.KeyValue) {
	if o == nil {
		return
	}
	hist.Record(o.ctx, value, metric.WithAttributes(attrs...))
}
