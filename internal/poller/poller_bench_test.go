package poller

import (
	"context"
	"testing"
	"time"

	"nba-data-service/internal/app/games"
	"nba-data-service/internal/domain"
	"nba-data-service/internal/store"
)

type benchProvider struct {
	games []domain.Game
}

func (b *benchProvider) FetchGames(ctx context.Context, date string, tz string) ([]domain.Game, error) {
	_ = ctx
	_ = date
	_ = tz
	return b.games, nil
}

func BenchmarkPollerFetchOnce(b *testing.B) {
	p := &benchProvider{
		games: []domain.Game{
			{
				ID:        "bench-game",
				Provider:  "fixture",
				HomeTeam:  domain.Team{ID: "home", Name: "Home"},
				AwayTeam:  domain.Team{ID: "away", Name: "Away"},
				StartTime: time.Date(2024, 1, 1, 19, 30, 0, 0, time.UTC).Format(time.RFC3339),
				Status:    domain.StatusFinal,
				Score:     domain.Score{Home: 100, Away: 95},
			},
		},
	}

	s := store.NewMemoryStore()
	svc := games.NewService(s)
	pl := New(p, svc, nil, nil, time.Second)
	ctx := context.Background()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pl.fetchOnce(ctx)
	}
}
