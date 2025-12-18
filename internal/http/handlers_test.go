package http

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"nba-games-service/internal/domain"
	"nba-games-service/internal/store"
)

type stubProvider struct {
	games []domain.Game
	err   error
}

func (s *stubProvider) FetchGames(ctx context.Context, date string, tz string) ([]domain.Game, error) {
	_ = ctx
	_ = date
	_ = tz
	return s.games, s.err
}

func TestHealth(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := domain.NewService(ms)
	h := NewHandler(svc, nil, nil)

	req := httptest.NewRequest("GET", "/health", nil)
	rr := httptest.NewRecorder()

	h.Health(rr, req)

	if rr.Code != 200 {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed decoding health response: %v", err)
	}
	if resp["status"] != "ok" {
		t.Fatalf("expected status ok, got %s", resp["status"])
	}
}

func TestGamesToday(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := domain.NewService(ms)
	game := domain.Game{
		ID:        "game-1",
		Provider:  "test",
		HomeTeam:  domain.Team{ID: "home", Name: "Home", ExternalID: 1},
		AwayTeam:  domain.Team{ID: "away", Name: "Away", ExternalID: 2},
		StartTime: time.Date(2024, 1, 1, 15, 30, 0, 0, time.UTC).Format(time.RFC3339),
		Status:    domain.StatusScheduled,
		Score:     domain.Score{Home: 0, Away: 0},
		Meta:      domain.GameMeta{Season: "2023-2024", UpstreamGameID: 123},
	}
	ms.SetGames([]domain.Game{game})

	h := NewHandler(svc, nil, nil)
	fixedNow := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	h.now = func() time.Time { return fixedNow }

	req := httptest.NewRequest("GET", "/games/today", nil)
	rr := httptest.NewRecorder()

	h.GamesToday(rr, req)

	if rr.Code != 200 {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp domain.TodayResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed decoding games response: %v", err)
	}

	if resp.Date != "2024-01-02" {
		t.Fatalf("expected date 2024-01-02, got %s", resp.Date)
	}

	if len(resp.Games) != 1 {
		t.Fatalf("expected 1 game, got %d", len(resp.Games))
	}

	if resp.Games[0].ID != "game-1" {
		t.Fatalf("unexpected game id %s", resp.Games[0].ID)
	}
}

func TestGamesTodayWithDateUsesProvider(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := domain.NewService(ms)

	provider := &stubProvider{
		games: []domain.Game{{ID: "provider-game"}},
	}

	h := NewHandler(svc, nil, provider)

	req := httptest.NewRequest("GET", "/games?date=2024-02-01", nil)
	rr := httptest.NewRecorder()

	h.GamesToday(rr, req)

	if rr.Code != 200 {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp domain.TodayResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed decoding games response: %v", err)
	}

	if resp.Date != "2024-02-01" {
		t.Fatalf("expected date to reflect query param, got %s", resp.Date)
	}
	if len(resp.Games) != 1 || resp.Games[0].ID != "provider-game" {
		t.Fatalf("expected provider games, got %+v", resp.Games)
	}
}

func TestGameByIDNotFound(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := domain.NewService(ms)
	h := NewHandler(svc, nil, nil)

	req := httptest.NewRequest("GET", "/games/unknown", nil)
	rr := httptest.NewRecorder()

	h.GameByID(rr, req)

	if rr.Code != 404 {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestGameByIDMissingID(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := domain.NewService(ms)
	h := NewHandler(svc, nil, nil)

	req := httptest.NewRequest("GET", "/games/", nil)
	rr := httptest.NewRecorder()

	h.GameByID(rr, req)

	if rr.Code != 400 {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestGameByIDSuccess(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := domain.NewService(ms)
	game := domain.Game{
		ID:        "game-1",
		Provider:  "test",
		HomeTeam:  domain.Team{ID: "home", Name: "Home", ExternalID: 1},
		AwayTeam:  domain.Team{ID: "away", Name: "Away", ExternalID: 2},
		StartTime: time.Date(2024, 1, 1, 15, 30, 0, 0, time.UTC).Format(time.RFC3339),
		Status:    domain.StatusScheduled,
		Score:     domain.Score{Home: 0, Away: 0},
		Meta:      domain.GameMeta{Season: "2023-2024", UpstreamGameID: 123},
	}
	ms.SetGames([]domain.Game{game})

	h := NewHandler(svc, nil, nil)

	req := httptest.NewRequest("GET", "/games/game-1", nil)
	rr := httptest.NewRecorder()

	h.GameByID(rr, req)

	if rr.Code != 200 {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp domain.Game
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.ID != "game-1" {
		t.Fatalf("unexpected game id %s", resp.ID)
	}
}
