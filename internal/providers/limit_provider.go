package providers

import (
	"context"
	"time"

	"log/slog"

	"github.com/preston-bernstein/nba-data-service/internal/domain/games"
)

// rateLimitedProvider wraps a GameProvider and enforces a minimum interval between calls.
type rateLimitedProvider struct {
	next     GameProvider
	interval time.Duration
	ticker   *time.Ticker
	logger   *slog.Logger
	name     string
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
		name:     "rate-limited",
	}
}

func (p *rateLimitedProvider) FetchGames(ctx context.Context, date string, tz string) ([]games.Game, error) {
	if p == nil || p.next == nil {
		logWithProvider(ctx, p.logger, slog.LevelWarn, p.name, "provider unavailable")
		return nil, ErrProviderUnavailable
	}
	select {
	case <-ctx.Done():
		logWithProvider(ctx, p.logger, slog.LevelWarn, p.name, "rate-limited fetch canceled")
		return nil, ctx.Err()
	case <-p.ticker.C:
	}
	logWithProvider(ctx, p.logger, slog.LevelInfo, p.name, "rate-limited provider fetch",
		slog.String("date", date),
		slog.String("tz", tz),
	)
	return p.next.FetchGames(ctx, date, tz)
}

// Close stops the internal ticker; callers should invoke when discarding the provider to avoid leaks.
func (p *rateLimitedProvider) Close() {
	if p != nil && p.ticker != nil {
		p.ticker.Stop()
	}
}
