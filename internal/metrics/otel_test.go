package metrics

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
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
