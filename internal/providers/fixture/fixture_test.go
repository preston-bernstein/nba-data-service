package fixture

import (
	"context"
	"testing"
	"time"
)

func TestFetchGamesReturnsDeterministicGames(t *testing.T) {
	fixed := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	p := New()
	p.now = func() time.Time { return fixed }

	games, err := p.FetchGames(context.Background(), "", "")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(games) != 2 {
		t.Fatalf("expected 2 games, got %d", len(games))
	}

	first := games[0]
	if first.ID != "fixture-1" || first.Provider != "fixture" {
		t.Fatalf("unexpected first game: %+v", first)
	}
	if first.StartTime != fixed.Truncate(time.Hour).Add(2*time.Hour).Format(time.RFC3339) {
		t.Fatalf("unexpected start time %s", first.StartTime)
	}
	if first.Meta.UpstreamGameID != 1001 {
		t.Fatalf("unexpected upstream id %d", first.Meta.UpstreamGameID)
	}
}

func TestNewCreatesProvider(t *testing.T) {
	p := New()
	if p == nil || p.now == nil {
		t.Fatalf("expected provider with now set")
	}

	fixed := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	p.now = func() time.Time { return fixed }
	games, err := p.FetchGames(context.Background(), "2024-02-10", "")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if games[0].StartTime[:10] != "2024-02-10" {
		t.Fatalf("expected date override, got %s", games[0].StartTime)
	}
}
