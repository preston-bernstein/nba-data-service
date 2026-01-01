package providers

import (
	"context"
	"time"

	"log/slog"

	"nba-data-service/internal/domain"
)

// rateLimitedProvider wraps a GameProvider and enforces a minimum interval between calls.
type rateLimitedProvider struct {
	next     GameProvider
	interval time.Duration
	ticker   *time.Ticker
	logger   *slog.Logger
}

// NewRateLimitedProvider returns a GameProvider that limits calls to the given interval.
// Calls block until the interval elapses to avoid exceeding upstream quotas.
func NewRateLimitedProvider(next GameProvider, interval time.Duration, logger *slog.Logger) GameProvider {
	if interval <= 0 {
		interval = time.Minute
	}
	return &rateLimitedProvider{
		next:     next,
		interval: interval,
		ticker:   time.NewTicker(interval),
		logger:   logger,
	}
}

func (p *rateLimitedProvider) FetchGames(ctx context.Context, date string, tz string) ([]domain.Game, error) {
	if p == nil || p.next == nil {
		if p.logger != nil {
			p.logger.Warn("provider unavailable", slog.String("provider", "rate-limited"))
		}
		return nil, ErrProviderUnavailable
	}
	select {
	case <-ctx.Done():
		if p.logger != nil {
			p.logger.Warn("rate-limited fetch canceled", slog.String("provider", "rate-limited"))
		}
		return nil, ctx.Err()
	case <-p.ticker.C:
	}
	if p.logger != nil {
		p.logger.Info("rate-limited provider fetch", slog.String("provider", "rate-limited"), slog.String("date", date))
	}
	return p.next.FetchGames(ctx, date, tz)
}
