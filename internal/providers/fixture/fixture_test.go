package fixture

import (
	"context"
	"testing"
	"time"
)

func TestFetchGamesReturnsDeterministicGames(t *testing.T) {
	fixed := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	p := &Provider{
		now: func() time.Time { return fixed },
	}

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
