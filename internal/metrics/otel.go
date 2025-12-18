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
		cfg.ServiceName = "nba-games-service"
	}

	promReader, promHandler, err := prometheusComponents()
	if err != nil {
		return nil, nil, nil, err
	}

	opts := []sdkmetric.Option{sdkmetric.WithReader(promReader)}

	if cfg.OtlpEndpoint != "" {
		otlpOpts := []otlpmetrichttp.Option{otlpmetrichttp.WithEndpoint(cfg.OtlpEndpoint)}
		if cfg.OtlpInsecure {
			otlpOpts = append(otlpOpts, otlpmetrichttp.WithInsecure())
		}
		otlpExp, err := otlpmetrichttp.New(ctx, otlpOpts...)
		if err != nil {
			return nil, nil, nil, err
		}
		opts = append(opts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(otlpExp, sdkmetric.WithInterval(15*time.Second))))
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(cfg.ServiceName)),
	)
	if err != nil {
		return nil, nil, nil, err
	}

	opts = append(opts, sdkmetric.WithResource(res))

	provider := sdkmetric.NewMeterProvider(opts...)

	otelInst, err := newOtelInstruments(provider)
	if err != nil {
		return nil, nil, nil, err
	}

	rec := newRecorder(otelInst)
	shutdown := func(c context.Context) error {
		return provider.Shutdown(c)
	}

	return rec, promHandler, shutdown, nil
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
		return nil, nil, err
	}
	return promExp, promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), nil
}

func newOtelInstruments(provider *sdkmetric.MeterProvider) (*otelInstruments, error) {
	meter := provider.Meter("nba-games-service")
	ctx := context.Background()

	requests, err := meter.Int64Counter("http_requests_total")
	if err != nil {
		return nil, err
	}
	requestLatency, err := meter.Float64Histogram("http_request_duration_ms")
	if err != nil {
		return nil, err
	}

	providerAttempts, err := meter.Int64Counter("provider_attempts_total")
	if err != nil {
		return nil, err
	}
	providerErrors, err := meter.Int64Counter("provider_errors_total")
	if err != nil {
		return nil, err
	}
	providerLatency, err := meter.Float64Histogram("provider_duration_ms")
	if err != nil {
		return nil, err
	}
	rateLimitHits, err := meter.Int64Counter("provider_rate_limit_hits_total")
	if err != nil {
		return nil, err
	}
	retryAfter, err := meter.Float64Histogram("provider_retry_after_ms")
	if err != nil {
		return nil, err
	}
	pollerCycles, err := meter.Int64Counter("poller_cycles_total")
	if err != nil {
		return nil, err
	}
	pollerErrors, err := meter.Int64Counter("poller_errors_total")
	if err != nil {
		return nil, err
	}
	pollerLatency, err := meter.Float64Histogram("poller_cycle_duration_ms")
	if err != nil {
		return nil, err
	}

	return &otelInstruments{
		ctx:               ctx,
		meter:             meter,
		requests:          requests,
		requestLatencyMs:  requestLatency,
		providerAttempts:  providerAttempts,
		providerErrors:    providerErrors,
		providerLatencyMs: providerLatency,
		rateLimitHits:     rateLimitHits,
		retryAfterMs:      retryAfter,
		pollerCycles:      pollerCycles,
		pollerErrors:      pollerErrors,
		pollerLatencyMs:   pollerLatency,
	}, nil
}

func (o *otelInstruments) recordHTTPRequest(method, path string, status int, duration time.Duration) {
	if o == nil {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("method", method),
		attribute.String("path", path),
		attribute.Int("status", status),
	}
	o.requests.Add(o.ctx, 1, metric.WithAttributes(attrs...))
	o.requestLatencyMs.Record(o.ctx, float64(duration.Milliseconds()), metric.WithAttributes(attrs...))
}

func (o *otelInstruments) recordProviderAttempt(provider string, duration time.Duration, err error) {
	if o == nil {
		return
	}
	attrs := []attribute.KeyValue{attribute.String("provider", provider)}
	o.providerAttempts.Add(o.ctx, 1, metric.WithAttributes(attrs...))
	o.providerLatencyMs.Record(o.ctx, float64(duration.Milliseconds()), metric.WithAttributes(attrs...))
	if err != nil {
		o.providerErrors.Add(o.ctx, 1, metric.WithAttributes(attrs...))
	}
}

func (o *otelInstruments) recordRateLimit(provider string, retryAfter time.Duration) {
	if o == nil {
		return
	}
	attrs := []attribute.KeyValue{attribute.String("provider", provider)}
	o.rateLimitHits.Add(o.ctx, 1, metric.WithAttributes(attrs...))
	if retryAfter > 0 {
		o.retryAfterMs.Record(o.ctx, float64(retryAfter.Milliseconds()), metric.WithAttributes(attrs...))
	}
}

func (o *otelInstruments) recordPoller(duration time.Duration, err error) {
	if o == nil {
		return
	}
	o.pollerCycles.Add(o.ctx, 1)
	o.pollerLatencyMs.Record(o.ctx, float64(duration.Milliseconds()))
	if err != nil {
		o.pollerErrors.Add(o.ctx, 1)
	}
}
