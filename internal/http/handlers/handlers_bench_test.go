package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"nba-data-service/internal/app/games"
	"nba-data-service/internal/domain"
	"nba-data-service/internal/snapshots"
	"nba-data-service/internal/store"
)

func BenchmarkGamesToday(b *testing.B) {
	ms := store.NewMemoryStore()
	now := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	ms.SetGames([]domain.Game{
		{
			ID:        "game-1",
			Provider:  "test",
			HomeTeam:  domain.Team{ID: "home", Name: "Home", ExternalID: 1},
			AwayTeam:  domain.Team{ID: "away", Name: "Away", ExternalID: 2},
			StartTime: now.Format(time.RFC3339),
			Status:    domain.StatusScheduled,
			Score:     domain.Score{Home: 0, Away: 0},
			Meta:      domain.GameMeta{Season: "2023-2024", UpstreamGameID: 123},
		},
	})
	svc := games.NewService(ms)
	h := NewHandler(svc, snapshots.NewFSStore(b.TempDir()), nil, nil)
	h.now = func() time.Time { return now }

	req := httptest.NewRequest(http.MethodGet, "/games/today", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr := httptest.NewRecorder()
		h.GamesToday(rr, req)
	}
}

func BenchmarkGameByID(b *testing.B) {
	ms := store.NewMemoryStore()
	now := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	ms.SetGames([]domain.Game{
		{
			ID:        "game-1",
			Provider:  "test",
			HomeTeam:  domain.Team{ID: "home", Name: "Home", ExternalID: 1},
			AwayTeam:  domain.Team{ID: "away", Name: "Away", ExternalID: 2},
			StartTime: now.Format(time.RFC3339),
			Status:    domain.StatusScheduled,
			Score:     domain.Score{Home: 0, Away: 0},
			Meta:      domain.GameMeta{Season: "2023-2024", UpstreamGameID: 123},
		},
	})
	svc := games.NewService(ms)
	h := NewHandler(svc, nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/games/game-1", nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr := httptest.NewRecorder()
		h.GameByID(rr, req)
	}
}
