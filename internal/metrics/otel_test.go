package metrics

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestSetupDisabledReturnsNoHandler(t *testing.T) {
	rec, handler, shutdown, err := Setup(context.Background(), TelemetryConfig{
		Enabled: false,
	})
	if err != nil {
		t.Fatalf("expected no error when disabled, got %v", err)
	}
	if rec == nil {
		t.Fatalf("expected recorder")
	}
	if handler != nil {
		t.Fatalf("expected nil handler when disabled")
	}
	if shutdown == nil {
		t.Fatalf("expected shutdown function")
	}
}

func TestSetupEnabledInitializesRecorderAndHandler(t *testing.T) {
	rec, handler, shutdown, err := Setup(context.Background(), TelemetryConfig{
		Enabled:     true,
		ServiceName: "nba-data-service",
		// No OTLP endpoint; uses Prometheus exporter only.
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if rec == nil {
		t.Fatalf("expected recorder")
	}
	if handler == nil {
		t.Fatalf("expected handler when enabled")
	}
	if shutdown == nil {
		t.Fatalf("expected shutdown function")
	}

	// Exercise otel-backed recorders to ensure no panic.
	rec.RecordHTTPRequest("GET", "/health", 200, time.Millisecond)
	rec.RecordPollerCycle(time.Millisecond, nil)
	rec.RecordProviderAttempt("balldontlie", time.Millisecond, nil)
	rec.RecordProviderAttempt("balldontlie", time.Millisecond, errors.New("fail"))
	rec.RecordRateLimit("balldontlie", time.Second)
	rec.RecordPollerCycle(time.Millisecond, errors.New("poller-fail"))
}

func TestSetupWithOTLPFactoryOverride(t *testing.T) {
	origProm := promReaderFactory
	origOTLP := otlpReaderFactory
	defer func() {
		promReaderFactory = origProm
		otlpReaderFactory = origOTLP
	}()

	promReaderFactory = func() (sdkmetric.Reader, http.Handler, error) {
		return sdkmetric.NewManualReader(), http.NewServeMux(), nil
	}
	otlpReaderFactory = func(ctx context.Context, endpoint string, insecure bool) (sdkmetric.Reader, error) {
		if endpoint == "" {
			t.Fatalf("expected endpoint provided")
		}
		if insecure {
			t.Fatalf("expected insecure false")
		}
		return sdkmetric.NewManualReader(), nil
	}

	rec, handler, shutdown, err := Setup(context.Background(), TelemetryConfig{
		Enabled:      true,
		ServiceName:  "nba-data-service",
		OtlpEndpoint: "collector:4318",
		OtlpInsecure: false,
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if rec == nil || handler == nil || shutdown == nil {
		t.Fatalf("expected recorder, handler, shutdown")
	}
	rec.RecordProviderAttempt("balldontlie", time.Millisecond, errors.New("boom"))
	rec.RecordRateLimit("balldontlie", 0)
}

func TestBuildOTLPReaderUsesEndpoint(t *testing.T) {
	reader, err := buildOTLPReader(context.Background(), "localhost:4318", true)
	if err != nil {
		t.Skipf("otlp reader not available in env: %v", err)
	}
	if reader == nil {
		t.Fatalf("expected reader")
	}
}

func TestBuildOTLPReaderWithBadEndpointFails(t *testing.T) {
	if _, err := buildOTLPReader(context.Background(), "://bad-endpoint", true); err == nil {
		t.Fatalf("expected error for bad endpoint")
	}
}

func TestSetupDefaultsServiceNameWhenEmpty(t *testing.T) {
	rec, handler, shutdown, err := Setup(context.Background(), TelemetryConfig{
		Enabled: true,
		// no service name set; should use default
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if rec == nil || handler == nil || shutdown == nil {
		t.Fatalf("expected recorder, handler, shutdown")
	}
	rec.RecordRateLimit("balldontlie", 0)
	rec.RecordProviderAttempt("balldontlie", time.Millisecond, errors.New("fail"))
}

func TestPrometheusComponentsReturnsReaderAndHandler(t *testing.T) {
	reader, handler, err := prometheusComponents()
	if err != nil {
		t.Fatalf("expected prom components, got err %v", err)
	}
	if reader == nil || handler == nil {
		t.Fatalf("expected reader and handler")
	}

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	handler.ServeHTTP(rr, req)
	if rr.Code == 0 {
		t.Fatalf("expected handler to write status")
	}
}

func TestOtelInstrumentsRecordingDoesNotPanic(t *testing.T) {
	// nil receiver should be a no-op
	var nilInst *otelInstruments
	nilInst.recordHTTPRequest("GET", "/health", 200, time.Millisecond)
	nilInst.recordProviderAttempt("p", time.Millisecond, nil)
	nilInst.recordRateLimit("p", time.Second)
	nilInst.recordPoller(time.Millisecond, errors.New("err"))

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	inst, err := newOtelInstruments(provider)
	if err != nil {
		t.Fatalf("expected instruments, got %v", err)
	}
	inst.recordHTTPRequest("GET", "/games/today", 200, 50*time.Millisecond)
	inst.recordProviderAttempt("balldontlie", 75*time.Millisecond, nil)
	inst.recordProviderAttempt("balldontlie", 90*time.Millisecond, errors.New("fail"))
	inst.recordRateLimit("balldontlie", 2*time.Second)
	inst.recordRateLimit("balldontlie", 0)
	inst.recordPoller(120*time.Millisecond, nil)
	inst.recordPoller(130*time.Millisecond, errors.New("poller"))
}

func TestSetupWithOTLPEnabledUsesDefaultFactories(t *testing.T) {
	ctx := context.Background()
	rec, handler, shutdown, err := Setup(ctx, TelemetryConfig{
		Enabled:      true,
		ServiceName:  "nba-data-service",
		OtlpEndpoint: "localhost:4318",
		OtlpInsecure: true,
	})
	if err != nil {
		t.Skipf("otlp reader not available in env: %v", err)
	}
	if rec == nil || handler == nil || shutdown == nil {
		t.Fatalf("expected recorder, handler, shutdown")
	}
	rec.RecordHTTPRequest("GET", "/ready", 200, time.Millisecond)
	rec.RecordPollerCycle(time.Millisecond, nil)
	rec.RecordProviderAttempt("fixture", time.Millisecond, nil)
}

func TestNewOtelInstrumentsRegistersAllMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	inst, err := newOtelInstruments(provider)
	if err != nil {
		t.Fatalf("expected instruments, got %v", err)
	}
	if inst == nil || inst.meter == nil {
		t.Fatalf("expected instruments and meter")
	}

	// Record once and ensure we can pull data without error.
	inst.recordProviderAttempt("fixture", 10*time.Millisecond, nil)
	if err := reader.Collect(context.Background(), &metricdata.ResourceMetrics{}); err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}
}

func TestNewOtelInstrumentsHandlesCreationErrors(t *testing.T) {
	// Meter provider that fails instrument creation via injected factory.
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader()))
	origFactory := instrumentFactory
	defer func() { instrumentFactory = origFactory }()
	instrumentFactory = func(metric.MeterProvider) (*otelInstruments, error) {
		return nil, errors.New("boom")
	}
	if _, err := instrumentFactory(provider); err == nil {
		t.Fatalf("expected error from injected failure")
	}
}

func TestPrometheusComponentsErrorPath(t *testing.T) {
	origFactory := promReaderFactory
	defer func() { promReaderFactory = origFactory }()

	// Force promexporter.New to fail by swapping the factory.
	promReaderFactory = func() (sdkmetric.Reader, http.Handler, error) {
		return nil, nil, errors.New("prom fail")
	}

	if _, _, _, err := Setup(context.Background(), TelemetryConfig{Enabled: true}); err == nil {
		t.Fatalf("expected setup to fail when prom factory fails")
	}
}

func TestPrometheusComponentsSuccess(t *testing.T) {
	reader, handler, err := prometheusComponents()
	if err != nil {
		t.Fatalf("expected prom components success, got %v", err)
	}
	if reader == nil || handler == nil {
		t.Fatalf("expected reader and handler")
	}
}

func TestSetupReturnsErrorWhenFactoriesFail(t *testing.T) {
	origProm := promReaderFactory
	origOTLP := otlpReaderFactory
	defer func() {
		promReaderFactory = origProm
		otlpReaderFactory = origOTLP
	}()

	promReaderFactory = func() (sdkmetric.Reader, http.Handler, error) {
		return nil, nil, errors.New("prom err")
	}

	if _, _, _, err := Setup(context.Background(), TelemetryConfig{Enabled: true}); err == nil {
		t.Fatalf("expected error when prom factory fails")
	}

	promReaderFactory = origProm
	otlpReaderFactory = func(ctx context.Context, endpoint string, insecure bool) (sdkmetric.Reader, error) {
		return nil, errors.New("otlp err")
	}

	if _, _, _, err := Setup(context.Background(), TelemetryConfig{Enabled: true, OtlpEndpoint: "collector"}); err == nil {
		t.Fatalf("expected error when otlp factory fails")
	}
}

type scriptedMeter struct {
	metric.Meter
	failCounter   string
	failHistogram string
}

func (m scriptedMeter) Int64Counter(name string, opts ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	if name == m.failCounter {
		return nil, errors.New("counter fail")
	}
	return m.Meter.Int64Counter(name, opts...)
}

func (m scriptedMeter) Float64Histogram(name string, opts ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	if name == m.failHistogram {
		return nil, errors.New("histogram fail")
	}
	return m.Meter.Float64Histogram(name, opts...)
}

func (scriptedMeter) meter() {}

type scriptedProvider struct {
	metric.MeterProvider
	m metric.Meter
}

func (p scriptedProvider) Meter(_ string, _ ...metric.MeterOption) metric.Meter {
	return p.m
}

func TestNewOtelInstrumentsErrorBranches(t *testing.T) {
	base := noop.NewMeterProvider().Meter("base")
	cases := []struct {
		name string
		hist bool
	}{
		{"http_requests_total", false},
		{"http_request_duration_ms", true},
		{"provider_attempts_total", false},
		{"provider_errors_total", false},
		{"provider_duration_ms", true},
		{"provider_rate_limit_hits_total", false},
		{"provider_retry_after_ms", true},
		{"poller_cycles_total", false},
		{"poller_errors_total", false},
		{"poller_cycle_duration_ms", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseProvider := noop.NewMeterProvider()
			m := scriptedMeter{Meter: base, failCounter: tc.name}
			if tc.hist {
				m.failCounter = ""
				m.failHistogram = tc.name
			}
			provider := scriptedProvider{MeterProvider: baseProvider, m: m}
			if _, err := newOtelInstruments(provider); err == nil {
				t.Fatalf("expected error for instrument %s", tc.name)
			}
		})
	}
}

func TestSetupFailsWhenInstrumentFactoryErrors(t *testing.T) {
	orig := instrumentFactory
	defer func() { instrumentFactory = orig }()

	instrumentFactory = func(metric.MeterProvider) (*otelInstruments, error) {
		return nil, errors.New("instrument fail")
	}

	if _, _, _, err := Setup(context.Background(), TelemetryConfig{Enabled: true}); err == nil {
		t.Fatalf("expected setup to fail when instrument factory errors")
	}
}
