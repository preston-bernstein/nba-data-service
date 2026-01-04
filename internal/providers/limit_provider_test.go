package providers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain/players"
	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
)

type stubProvider struct {
	calls int
	err   error
}

func (p *stubProvider) FetchGames(ctx context.Context, date string, tz string) ([]games.Game, error) {
	p.calls++
	return []games.Game{{ID: "g1"}}, p.err
}

func (p *stubProvider) FetchTeams(ctx context.Context) ([]teams.Team, error) {
	p.calls++
	return []teams.Team{{ID: "t1"}}, p.err
}

func (p *stubProvider) FetchPlayers(ctx context.Context) ([]players.Player, error) {
	p.calls++
	return []players.Player{{ID: "p1"}}, p.err
}

func TestRateLimitedProviderBlocksUntilTick(t *testing.T) {
	inner := &stubProvider{}
	rl := NewRateLimitedProvider(inner, 5*time.Millisecond, nil).(*rateLimitedProvider)

	start := time.Now()
	if _, err := rl.FetchGames(context.Background(), "2024-01-01", ""); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 5*time.Millisecond {
		t.Fatalf("expected call to wait for ticker, elapsed %s", elapsed)
	}
	if inner.calls != 1 {
		t.Fatalf("expected inner provider called once, got %d", inner.calls)
	}
}

func TestRateLimitedProviderRespectsCanceledContext(t *testing.T) {
	inner := &stubProvider{}
	rl := NewRateLimitedProvider(inner, time.Minute, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := rl.FetchGames(ctx, "2024-01-01", ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled error, got %v", err)
	}
	if inner.calls != 0 {
		t.Fatalf("expected inner provider not called on canceled context")
	}
}

func TestRateLimitedProviderHandlesNilInner(t *testing.T) {
	var inner GameProvider
	rl := NewRateLimitedProvider(inner, time.Millisecond, nil)

	_, err := rl.FetchGames(context.Background(), "2024-01-01", "")
	if !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("expected ErrProviderUnavailable, got %v", err)
	}
}

func TestRateLimitedProviderSupportsTeamsAndPlayers(t *testing.T) {
	inner := &stubProvider{}
	rl := NewRateLimitedProvider(inner, time.Millisecond, nil).(*rateLimitedProvider)

	if _, err := rl.FetchTeams(context.Background()); err != nil {
		t.Fatalf("expected teams without error, got %v", err)
	}
	if _, err := rl.FetchPlayers(context.Background()); err != nil {
		t.Fatalf("expected players without error, got %v", err)
	}
	if inner.calls != 2 {
		t.Fatalf("expected teams and players to be invoked, got %d calls", inner.calls)
	}
}

func TestRateLimitedProviderCloseStopsTicker(t *testing.T) {
	rl := NewRateLimitedProvider(&stubProvider{}, time.Millisecond, nil).(*rateLimitedProvider)
	rl.Close() // ensure no panic and ticker stopped
}

func TestRateLimitedProviderDefaultsInterval(t *testing.T) {
	rl := NewRateLimitedProvider(&stubProvider{}, 0, nil).(*rateLimitedProvider)
	if rl.interval != time.Minute {
		t.Fatalf("expected default interval 1m, got %s", rl.interval)
	}
	rl.Close()
}

func TestRateLimitedProviderRespectsCanceledContextForTeamsAndPlayers(t *testing.T) {
	inner := &stubProvider{}
	rl := NewRateLimitedProvider(inner, time.Hour, nil).(*rateLimitedProvider)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := rl.FetchTeams(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled for teams, got %v", err)
	}
	if _, err := rl.FetchPlayers(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled for players, got %v", err)
	}
	if inner.calls != 0 {
		t.Fatalf("expected no calls to inner providers")
	}
}

type gamesOnlyProvider struct{}

func (g *gamesOnlyProvider) FetchGames(ctx context.Context, date string, tz string) ([]games.Game, error) {
	return nil, nil
}

func TestRateLimitedProviderHandlesMissingTeamOrPlayerProvider(t *testing.T) {
	rl := NewRateLimitedProvider(&gamesOnlyProvider{}, time.Millisecond, nil).(*rateLimitedProvider)

	if _, err := rl.FetchTeams(context.Background()); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("expected ErrProviderUnavailable for teams, got %v", err)
	}
	if _, err := rl.FetchPlayers(context.Background()); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("expected ErrProviderUnavailable for players, got %v", err)
	}
}
