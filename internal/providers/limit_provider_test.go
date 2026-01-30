package providers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/preston-bernstein/nba-data-service/internal/teststubs"
)

func TestRateLimitedProviderBlocksUntilTick(t *testing.T) {
	inner := &teststubs.StubProvider{}
	rl := NewRateLimitedProvider(inner, 5*time.Millisecond, nil).(*rateLimitedProvider)

	start := time.Now()
	if _, err := rl.FetchGames(context.Background(), "2024-01-01", ""); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 5*time.Millisecond {
		t.Fatalf("expected call to wait for ticker, elapsed %s", elapsed)
	}
	if inner.Calls.Load() != 1 {
		t.Fatalf("expected inner provider called once, got %d", inner.Calls.Load())
	}
}

func TestRateLimitedProviderRespectsCanceledContext(t *testing.T) {
	inner := &teststubs.StubProvider{}
	rl := NewRateLimitedProvider(inner, time.Minute, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := rl.FetchGames(ctx, "2024-01-01", ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled error, got %v", err)
	}
	if inner.Calls.Load() != 0 {
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

func TestRateLimitedProviderCloseStopsTicker(t *testing.T) {
	rl := NewRateLimitedProvider(&teststubs.StubProvider{}, time.Millisecond, nil).(*rateLimitedProvider)
	rl.Close() // ensure no panic and ticker stopped
}

func TestRateLimitedProviderDefaultsInterval(t *testing.T) {
	rl := NewRateLimitedProvider(&teststubs.StubProvider{}, 0, nil).(*rateLimitedProvider)
	if rl.interval != time.Minute {
		t.Fatalf("expected default interval 1m, got %s", rl.interval)
	}
	rl.Close()
}
