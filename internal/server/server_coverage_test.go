package server

import (
	"context"
	"net/http"
	"testing"

	"nba-data-service/internal/config"
	"nba-data-service/internal/metrics"
)

// metricsSetupSuccess allows us to force a handler to test buildMetrics success path.
func metricsSetupSuccess(ctx context.Context, cfg metrics.TelemetryConfig) (*metrics.Recorder, http.Handler, func(context.Context) error, error) {
	rec := metrics.NewRecorder()
	return rec, http.NewServeMux(), func(context.Context) error { return nil }, nil
}

func TestBuildMetricsSuccessPathSetsServerAndShutdown(t *testing.T) {
	orig := metricsSetup
	defer func() { metricsSetup = orig }()
	metricsSetup = metricsSetupSuccess

	rec, srv, stop := buildMetrics(config.Config{
		Metrics: config.MetricsConfig{
			Enabled: true,
			Port:    "9999",
		},
	}, nil, nil)

	if rec == nil || srv == nil || stop == nil {
		t.Fatalf("expected recorder, server, and shutdown to be set on success")
	}
}
