package server

import (
	"log/slog"
	"time"

	"nba-data-service/internal/config"
	"nba-data-service/internal/metrics"
	"nba-data-service/internal/providers"
)

// providerFactory assembles the provider with shared wrappers (rate limit + retry).
type providerFactory struct {
	logger  *slog.Logger
	metrics *metrics.Recorder
}

func newProviderFactory(logger *slog.Logger, metrics *metrics.Recorder) providerFactory {
	return providerFactory{logger: logger, metrics: metrics}
}

func (f providerFactory) build(cfg config.Config) providers.GameProvider {
	base := selectProvider(cfg, f.logger)
	// Shared rate limiter to respect upstream quota (1/min default if poll interval is shorter).
	limited := providers.NewRateLimitedProvider(base, time.Minute, f.logger)
	return providers.NewRetryingProvider(limited, f.logger, f.metrics, normalizeProviderName(cfg.Provider, base), 0, 0)
}
