package providers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/preston-bernstein/nba-data-service/internal/domain"
)

type stubProvider struct {
	calls int
	err   error
}

func (p *stubProvider) FetchGames(ctx context.Context, date string, tz string) ([]domain.Game, error) {
	p.calls++
	return []domain.Game{{ID: "g1"}}, p.err
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
