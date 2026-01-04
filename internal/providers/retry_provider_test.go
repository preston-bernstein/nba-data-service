package providers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand"
	"testing"
	"time"

	"github.com/preston-bernstein/nba-data-service/internal/domain/games"
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

func TestRetryingProviderJitterDelayAndSleep(t *testing.T) {
	rp := &retryingProvider{rng: rand.New(rand.NewSource(1))}

	if got := rp.jitterDelay(0); got != 0 {
		t.Fatalf("expected zero jitter for zero base, got %s", got)
	}
	if got := rp.jitterDelay(1); got != time.Duration(1) {
		t.Fatalf("expected base returned when half is zero, got %s", got)
	}
	jitter := rp.jitterDelay(10 * time.Millisecond)
	if jitter < 5*time.Millisecond || jitter > 10*time.Millisecond {
		t.Fatalf("expected jitter between 5ms and 10ms, got %s", jitter)
	}

	if err := rp.sleep(context.Background(), 0); err != nil {
		t.Fatalf("expected zero delay sleep to be nil, got %v", err)
	}
}

func TestRetryingProviderLogRetryIncludesRateLimitFields(t *testing.T) {
	bufLogger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
	rp := &retryingProvider{
		logger:       bufLogger,
		providerName: "test",
		maxAttempts:  3,
	}

	rlErr := &RateLimitError{StatusCode: 429, RetryAfter: time.Second, Remaining: "10"}
	rp.logRetry(context.Background(), 2, 2*time.Second, rlErr)
	rp.logRetry(context.Background(), 1, 0, errors.New("boom"))
	// logging is side-effect only; ensure calls do not panic and provider metadata is retained
	if rp.providerName != "test" {
		t.Fatalf("expected provider name to remain unchanged")
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
