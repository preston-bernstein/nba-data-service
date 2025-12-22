package http

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"nba-games-service/internal/domain"
	"nba-games-service/internal/store"
)

func BenchmarkGamesToday(b *testing.B) {
	ms := store.NewMemoryStore()
	svc := domain.NewService(ms)
	ms.SetGames([]domain.Game{
		{
			ID:        "game-1",
			Provider:  "fixture",
			HomeTeam:  domain.Team{ID: "home", Name: "Home"},
			AwayTeam:  domain.Team{ID: "away", Name: "Away"},
			StartTime: time.Date(2024, 1, 1, 19, 30, 0, 0, time.UTC).Format(time.RFC3339),
			Status:    domain.StatusFinal,
			Score:     domain.Score{Home: 100, Away: 95},
		},
	})

	h := NewHandler(svc, nil, nil, nil)
	h.now = func() time.Time { return time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC) }

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req := httptest.NewRequest("GET", "/games/today", nil).WithContext(context.Background())
			rr := httptest.NewRecorder()
			h.GamesToday(rr, req)
			if rr.Code != 200 {
				b.Fatalf("unexpected status %d", rr.Code)
			}
		}
	})
}

func BenchmarkGameByID(b *testing.B) {
	ms := store.NewMemoryStore()
	svc := domain.NewService(ms)
	game := domain.Game{
		ID:        "game-1",
		Provider:  "fixture",
		HomeTeam:  domain.Team{ID: "home", Name: "Home"},
		AwayTeam:  domain.Team{ID: "away", Name: "Away"},
		StartTime: time.Date(2024, 1, 1, 19, 30, 0, 0, time.UTC).Format(time.RFC3339),
		Status:    domain.StatusFinal,
		Score:     domain.Score{Home: 100, Away: 95},
	}
	ms.SetGames([]domain.Game{game})

	h := NewHandler(svc, nil, nil, nil)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req := httptest.NewRequest("GET", "/games/"+game.ID, nil).WithContext(context.Background())
			rr := httptest.NewRecorder()
			h.GameByID(rr, req)
			if rr.Code != 200 {
				b.Fatalf("unexpected status %d", rr.Code)
			}
		}
	})
}
