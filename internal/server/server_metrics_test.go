package server

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/preston-bernstein/nba-data-service/internal/config"
	"github.com/preston-bernstein/nba-data-service/internal/domain"
	"github.com/preston-bernstein/nba-data-service/internal/metrics"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
	"github.com/preston-bernstein/nba-data-service/internal/testutil"
)

type nopProvider struct{}

func (nopProvider) FetchGames(ctx context.Context, date string, tz string) ([]domain.Game, error) {
	return nil, nil
}

func TestNewServerWithMetricsHandlesSetupFailure(t *testing.T) {
	origSetup := metricsSetup
	defer func() { metricsSetup = origSetup }()

	metricsSetup = func(ctx context.Context, cfg metrics.TelemetryConfig) (*metrics.Recorder, http.Handler, func(context.Context) error, error) {
		return nil, nil, nil, errors.New("fail")
	}

	cfg := config.Config{
		Metrics:  config.MetricsConfig{Enabled: true},
		Provider: "fixture",
	}

	srv := newServerWithMetrics(cfg, nil, providers.NewRetryingProvider(nil, nil, nil, "", 0, 0), nil)
	if srv.metrics == nil {
		t.Fatalf("expected fallback metrics recorder even on setup failure")
	}
}

func TestNewServerWithMetricsDisabledSkipsSetup(t *testing.T) {
	cfg := config.Config{
		Metrics:  config.MetricsConfig{Enabled: false},
		Provider: "fixture",
	}

	srv := newServerWithMetrics(cfg, nil, providers.NewRetryingProvider(nil, nil, nil, "", 0, 0), nil)
	if srv.metrics == nil {
		t.Fatalf("expected recorder to be set even when metrics disabled")
	}
}

func TestNewServerWithMetricsUsesInjectedRecorder(t *testing.T) {
	rec, shutdown := testutil.NewRecorderWithShutdown()
	cfg := config.Config{
		Metrics:  config.MetricsConfig{Enabled: true},
		Provider: "fixture",
	}

	srv := newServerWithMetrics(cfg, nil, providers.NewRetryingProvider(nil, nil, nil, "", 0, 0), rec)
	if srv.metrics != rec {
		t.Fatalf("expected injected recorder to be used")
	}
	if srv.metricsStop != nil {
		if err := srv.metricsStop(context.Background()); err != nil {
			t.Fatalf("expected injected shutdown to succeed, got %v", err)
		}
	}
	_ = shutdown
}
