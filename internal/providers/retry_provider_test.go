package providers

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"testing"
	"time"

	"github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain/players"
	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
	"github.com/preston-bernstein/nba-data-service/internal/metrics"
)

type flakeyProvider struct {
	failures int
	calls    int
}

func (f *flakeyProvider) FetchGames(ctx context.Context, date string, tz string) ([]games.Game, error) {
	_ = ctx
	_ = date
	_ = tz
	f.calls++
	if f.calls <= f.failures {
		return nil, errors.New("boom")
	}
	return []games.Game{{ID: "ok"}}, nil
}

func TestRetryingProviderRetriesAndSucceeds(t *testing.T) {
	fp := &flakeyProvider{failures: 2}
	rp := NewRetryingProvider(fp, slog.Default(), metrics.NewRecorder(), "flakey", 3, 1*time.Millisecond)

	games, err := rp.FetchGames(context.Background(), "", "")
	if err != nil {
		t.Fatalf("expected success, got error %v", err)
	}
	if len(games) != 1 || games[0].ID != "ok" {
		t.Fatalf("unexpected games %+v", games)
	}
	if fp.calls != 3 {
		t.Fatalf("expected 3 attempts, got %d", fp.calls)
	}
}

func TestRetryingProviderStopsAfterMaxAttempts(t *testing.T) {
	fp := &flakeyProvider{failures: 5}
	rp := NewRetryingProvider(fp, nil, metrics.NewRecorder(), "flakey", 2, 1*time.Millisecond)

	_, err := rp.FetchGames(context.Background(), "", "")
	if err == nil {
		t.Fatal("expected error after retries")
	}
	if fp.calls != 2 {
		t.Fatalf("expected 2 attempts, got %d", fp.calls)
	}
}

func TestRetryingProviderRespectsContextCancel(t *testing.T) {
	fp := &flakeyProvider{failures: 5}
	rp := NewRetryingProvider(fp, nil, metrics.NewRecorder(), "flakey", 3, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := rp.FetchGames(ctx, "", "")
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestRetryingProviderUsesCustomBackoff(t *testing.T) {
	fp := &flakeyProvider{failures: 1}
	rp := NewRetryingProvider(fp, nil, metrics.NewRecorder(), "flakey", 2, time.Hour).(*retryingProvider)

	calls := 0
	rp.backoffFn = func(attempt int) time.Duration {
		calls++
		return 0
	}

	_, _ = rp.FetchGames(context.Background(), "", "")

	if calls == 0 {
		t.Fatalf("expected custom backoff to be invoked")
	}
}

func TestRetryingProviderRecordsRateLimitMetrics(t *testing.T) {
	rec := metrics.NewRecorder()
	rp := NewRetryingProvider(&rateLimitThenSuccessProvider{}, nil, rec, "rl", 2, time.Millisecond).(*retryingProvider)
	rp.backoffFn = func(attempt int) time.Duration {
		_ = attempt
		return 0 // avoid sleep in tests
	}

	games, err := rp.FetchGames(context.Background(), "", "")
	if err != nil {
		t.Fatalf("expected success after retry, got %v", err)
	}
	if len(games) != 1 || games[0].ID != "ok" {
		t.Fatalf("unexpected games %+v", games)
	}

	if got := rec.RateLimitHits(rp.providerName); got != 1 {
		t.Fatalf("expected 1 rate limit hit, got %d", got)
	}
	if got := rec.ProviderCalls(rp.providerName); got != 2 {
		t.Fatalf("expected 2 provider calls, got %d", got)
	}
	if got := rec.ProviderErrors(rp.providerName); got != 1 {
		t.Fatalf("expected 1 error, got %d", got)
	}
}

func TestRetryingProviderDelaySelection(t *testing.T) {
	rec := metrics.NewRecorder()
	rp := NewRetryingProvider(&rateLimitThenSuccessProvider{}, nil, rec, "rl", 2, time.Millisecond).(*retryingProvider)
	rp.rng = rand.New(rand.NewSource(1))
	rp.backoffFn = func(attempt int) time.Duration {
		_ = attempt
		return 50 * time.Millisecond
	}

	tests := []struct {
		name     string
		err      error
		expected time.Duration
	}{
		{
			name:     "rate_limit_uses_retry_after",
			err:      &RateLimitError{RetryAfter: 3 * time.Second},
			expected: 3 * time.Second,
		},
		{
			name:     "generic_error_uses_backoff_with_jitter",
			err:      errors.New("boom"),
			expected: 0, // non-zero but best-effort check >= base/2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delay := rp.computeDelay(tt.err, 1)
			if rlErr, ok := tt.err.(*RateLimitError); ok && rlErr.RetryAfter > 0 {
				if delay != tt.expected {
					t.Fatalf("expected retry-after delay %s, got %s", tt.expected, delay)
				}
				return
			}

			if delay <= 0 {
				t.Fatalf("expected positive delay for generic error, got %s", delay)
			}
			if delay < 25*time.Millisecond || delay > 50*time.Millisecond {
				t.Fatalf("expected jittered delay between 25ms and 50ms, got %s", delay)
			}
		})
	}
}

func TestNewRetryingProviderWithRNG(t *testing.T) {
	fp := &flakeyProvider{failures: 1}
	rng := rand.New(rand.NewSource(2))
	rp := NewRetryingProviderWithRNG(fp, nil, metrics.NewRecorder(), "flakey", rng, 2, time.Millisecond)

	games, err := rp.FetchGames(context.Background(), "", "")
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if len(games) != 1 {
		t.Fatalf("expected games from provider")
	}
}

func TestNewRetryingProviderWithDefaultRNG(t *testing.T) {
	fp := &flakeyProvider{failures: 0}
	rp := NewRetryingProviderWithRNG(fp, nil, metrics.NewRecorder(), "flakey", nil, 0, 0)
	games, err := rp.FetchGames(context.Background(), "", "")
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if len(games) != 1 {
		t.Fatalf("expected games from provider")
	}
}

func TestNewRetryingProviderWithNilProviderSetsFallbackName(t *testing.T) {
	rp := NewRetryingProviderWithRNG(nil, nil, metrics.NewRecorder(), "", nil, 0, 0).(*retryingProvider)
	if rp.providerName != "provider" {
		t.Fatalf("expected fallback provider name, got %s", rp.providerName)
	}
	if rp.maxAttempts != defaultRetryAttempts {
		t.Fatalf("expected default attempts, got %d", rp.maxAttempts)
	}
	if rp.backoffFn(1) != defaultBackoff {
		t.Fatalf("expected default backoff")
	}
}

type rateLimitThenSuccessProvider struct {
	calls int
}

func (f *rateLimitThenSuccessProvider) FetchGames(ctx context.Context, date string, tz string) ([]games.Game, error) {
	_ = ctx
	_ = date
	_ = tz
	f.calls++
	if f.calls == 1 {
		return nil, &RateLimitError{
			Provider:   "test",
			StatusCode: 429,
		}
	}
	return []games.Game{{ID: "ok"}}, nil
}

type alwaysFailTeamProvider struct{}

func (a *alwaysFailTeamProvider) FetchGames(ctx context.Context, date string, tz string) ([]games.Game, error) {
	return nil, errors.New("games not supported")
}

func (a *alwaysFailTeamProvider) FetchTeams(ctx context.Context) ([]teams.Team, error) {
	return nil, errors.New("fail teams")
}

func (a *alwaysFailTeamProvider) FetchPlayers(ctx context.Context) ([]players.Player, error) {
	return nil, errors.New("fail players")
}

func TestRetryingProviderRetriesTeamsAndPlayers(t *testing.T) {
	rp := NewRetryingProvider(&alwaysFailTeamProvider{}, nil, metrics.NewRecorder(), "multi", 2, time.Millisecond).(*retryingProvider)
	rp.backoffFn = func(attempt int) time.Duration {
		_ = attempt
		return 0
	}

	if _, err := rp.FetchTeams(context.Background()); err == nil {
		t.Fatalf("expected team fetch to fail after retries")
	}
	if _, err := rp.FetchPlayers(context.Background()); err == nil {
		t.Fatalf("expected player fetch to fail after retries")
	}
}

type staticTeamPlayerProvider struct{}

func (s *staticTeamPlayerProvider) FetchGames(ctx context.Context, date string, tz string) ([]games.Game, error) {
	return nil, errors.New("games not supported")
}
func (s *staticTeamPlayerProvider) FetchTeams(ctx context.Context) ([]teams.Team, error) {
	return []teams.Team{{ID: "t1"}}, nil
}
func (s *staticTeamPlayerProvider) FetchPlayers(ctx context.Context) ([]players.Player, error) {
	return []players.Player{{ID: "p1"}}, nil
}

func TestRetryingProviderTeamsAndPlayersSuccess(t *testing.T) {
	rec := metrics.NewRecorder()
	rp := NewRetryingProvider(&staticTeamPlayerProvider{}, nil, rec, "multi", 2, time.Millisecond).(*retryingProvider)

	teams, err := rp.FetchTeams(context.Background())
	if err != nil || len(teams) != 1 {
		t.Fatalf("expected teams success, got err=%v teams=%v", err, teams)
	}
	players, err := rp.FetchPlayers(context.Background())
	if err != nil || len(players) != 1 {
		t.Fatalf("expected players success, got err=%v players=%v", err, players)
	}
	if got := rec.ProviderCalls("multi"); got == 0 {
		t.Fatalf("expected metrics recorded")
	}
}

func TestRetryingProviderContextCancelForTeamsAndPlayers(t *testing.T) {
	rec := metrics.NewRecorder()
	rp := NewRetryingProvider(&alwaysFailTeamProvider{}, nil, rec, "multi", 3, time.Second).(*retryingProvider)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := rp.FetchTeams(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled context for teams, got %v", err)
	}
	if _, err := rp.FetchPlayers(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled context for players, got %v", err)
	}
}

type gamesOnlyRetryProvider struct{}

func (g *gamesOnlyRetryProvider) FetchGames(ctx context.Context, date string, tz string) ([]games.Game, error) {
	return nil, nil
}

func TestRetryingProviderReturnsUnavailableWhenTeamsOrPlayersUnsupported(t *testing.T) {
	rp := NewRetryingProvider(&gamesOnlyRetryProvider{}, nil, metrics.NewRecorder(), "games-only", 1, time.Millisecond).(*retryingProvider)
	if _, err := rp.FetchTeams(context.Background()); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("expected ErrProviderUnavailable for teams, got %v", err)
	}
	if _, err := rp.FetchPlayers(context.Background()); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("expected ErrProviderUnavailable for players, got %v", err)
	}
}
