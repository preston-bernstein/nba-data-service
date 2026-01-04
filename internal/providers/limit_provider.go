package providers

import (
	"context"
	"time"

	"log/slog"

	"github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain/players"
	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
)

// rateLimitedProvider wraps a GameProvider and enforces a minimum interval between calls.
type rateLimitedProvider struct {
	nextGame   GameProvider
	nextTeam   TeamProvider
	nextPlayer PlayerProvider
	interval   time.Duration
	ticker     *time.Ticker
	logger     *slog.Logger
	name       string
}

// NewRateLimitedProvider returns a GameProvider that limits calls to the given interval.
// Calls block until the interval elapses to avoid exceeding upstream quotas.
func NewRateLimitedProvider(next GameProvider, interval time.Duration, logger *slog.Logger) GameProvider {
	if interval <= 0 {
		interval = time.Minute
	}
	return &rateLimitedProvider{
		nextGame:   next,
		nextTeam:   asTeamProvider(next),
		nextPlayer: asPlayerProvider(next),
		interval:   interval,
		ticker:     time.NewTicker(interval),
		logger:     logger,
		name:       "rate-limited",
	}
}

func (p *rateLimitedProvider) FetchGames(ctx context.Context, date string, tz string) ([]games.Game, error) {
	if p == nil || p.nextGame == nil {
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
	return p.nextGame.FetchGames(ctx, date, tz)
}

func (p *rateLimitedProvider) FetchTeams(ctx context.Context) ([]teams.Team, error) {
	if p == nil || p.nextTeam == nil {
		logWithProvider(ctx, p.logger, slog.LevelWarn, p.name, "provider unavailable")
		return nil, ErrProviderUnavailable
	}
	select {
	case <-ctx.Done():
		logWithProvider(ctx, p.logger, slog.LevelWarn, p.name, "rate-limited fetch canceled")
		return nil, ctx.Err()
	case <-p.ticker.C:
	}
	logWithProvider(ctx, p.logger, slog.LevelInfo, p.name, "rate-limited provider fetch")
	return p.nextTeam.FetchTeams(ctx)
}

func (p *rateLimitedProvider) FetchPlayers(ctx context.Context) ([]players.Player, error) {
	if p == nil || p.nextPlayer == nil {
		logWithProvider(ctx, p.logger, slog.LevelWarn, p.name, "provider unavailable")
		return nil, ErrProviderUnavailable
	}
	select {
	case <-ctx.Done():
		logWithProvider(ctx, p.logger, slog.LevelWarn, p.name, "rate-limited fetch canceled")
		return nil, ctx.Err()
	case <-p.ticker.C:
	}
	logWithProvider(ctx, p.logger, slog.LevelInfo, p.name, "rate-limited provider fetch")
	return p.nextPlayer.FetchPlayers(ctx)
}

// Close stops the internal ticker; callers should invoke when discarding the provider to avoid leaks.
func (p *rateLimitedProvider) Close() {
	if p != nil && p.ticker != nil {
		p.ticker.Stop()
	}
}

func asTeamProvider(gp GameProvider) TeamProvider {
	if tp, ok := gp.(TeamProvider); ok {
		return tp
	}
	return nil
}

func asPlayerProvider(gp GameProvider) PlayerProvider {
	if pp, ok := gp.(PlayerProvider); ok {
		return pp
	}
	return nil
}
