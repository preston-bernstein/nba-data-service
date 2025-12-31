package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"nba-data-service/internal/app/games"
	"nba-data-service/internal/domain"
	"nba-data-service/internal/http/middleware"
	"nba-data-service/internal/poller"
	"nba-data-service/internal/store"
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
	svc := games.NewService(ms)
	h := NewHandler(svc, nil, nil, nil)

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

func TestHealthShuttingDownReturnsServiceUnavailable(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := games.NewService(ms)
	h := NewHandler(svc, nil, nil, nil)

	req := httptest.NewRequest("GET", "/health", nil)
	ctx, cancel := context.WithCancel(req.Context())
	cancel()
	req = req.WithContext(ctx)
	rr := httptest.NewRecorder()

	h.Health(rr, req)

	if rr.Code != 503 {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
	var resp map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed decoding health response: %v", err)
	}
	if resp["error"] != "shutting down" {
		t.Fatalf("unexpected error %q", resp["error"])
	}
}

func TestGamesToday(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := games.NewService(ms)
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

	h := NewHandler(svc, nil, nil, nil)
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
	svc := games.NewService(ms)

	provider := &stubProvider{
		games: []domain.Game{{ID: "provider-game"}},
	}

	h := NewHandler(svc, nil, provider, nil)

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

func TestGamesTodayWithInvalidDateReturnsBadRequest(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := games.NewService(ms)
	h := NewHandler(svc, nil, &stubProvider{}, nil)

	req := httptest.NewRequest("GET", "/games?date=not-a-date", nil)
	rr := httptest.NewRecorder()

	h.GamesToday(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestGamesTodayLogsUpstreamErrors(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := games.NewService(ms)

	provider := &stubProvider{
		err: context.DeadlineExceeded,
	}

	h := NewHandler(svc, nil, provider, nil)

	req := httptest.NewRequest("GET", "/games?date=2024-02-01", nil)
	rr := httptest.NewRecorder()

	h.GamesToday(rr, req)

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d", rr.Code)
	}
}

func TestGamesTodayInvalidTimezoneFallsBack(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := games.NewService(ms)
	h := NewHandler(svc, nil, nil, nil)

	fixedNow := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	h.now = func() time.Time { return fixedNow }

	req := httptest.NewRequest("GET", "/games/today?tz=invalid-timezone", nil)
	rr := httptest.NewRecorder()

	h.GamesToday(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp domain.TodayResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed decoding games response: %v", err)
	}

	if resp.Date != "2024-01-02" {
		t.Fatalf("expected date 2024-01-02, got %s", resp.Date)
	}
}

func TestGameByID(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := games.NewService(ms)

	game := domain.Game{
		ID:        "id-1",
		Provider:  "test",
		HomeTeam:  domain.Team{ID: "home", Name: "Home", ExternalID: 1},
		AwayTeam:  domain.Team{ID: "away", Name: "Away", ExternalID: 2},
		StartTime: time.Date(2024, 1, 1, 15, 30, 0, 0, time.UTC).Format(time.RFC3339),
		Status:    domain.StatusScheduled,
		Score:     domain.Score{Home: 0, Away: 0},
		Meta:      domain.GameMeta{Season: "2023-2024", UpstreamGameID: 123},
	}
	ms.SetGames([]domain.Game{game})

	h := NewHandler(svc, nil, nil, nil)

	req := httptest.NewRequest("GET", "/games/id-1", nil)
	rr := httptest.NewRecorder()

	h.GameByID(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp domain.Game
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed decoding game response: %v", err)
	}

	if resp.ID != "id-1" {
		t.Fatalf("expected game id id-1, got %s", resp.ID)
	}
}

func TestGameByIDInvalid(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := games.NewService(ms)
	h := NewHandler(svc, nil, nil, nil)

	req := httptest.NewRequest("GET", "/games", nil)
	rr := httptest.NewRecorder()

	h.GameByID(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestGameByIDNotFound(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := games.NewService(ms)
	h := NewHandler(svc, nil, nil, nil)

	req := httptest.NewRequest("GET", "/games/unknown", nil)
	rr := httptest.NewRecorder()

	h.GameByID(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestMethodNotAllowedHandlers(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := games.NewService(ms)
	h := NewHandler(svc, nil, nil, nil)

	tests := []struct {
		name string
		path string
		fn   func(w http.ResponseWriter, r *http.Request)
	}{
		{"health", "/health", h.Health},
		{"ready", "/ready", h.Ready},
		{"gamesToday", "/games/today", h.GamesToday},
		{"gameByID", "/games/id", h.GameByID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			rr := httptest.NewRecorder()
			tt.fn(rr, req)
			if rr.Code != http.StatusMethodNotAllowed {
				t.Fatalf("expected 405, got %d", rr.Code)
			}
		})
	}
}

func TestRequestIDPropagatesThroughMiddleware(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := games.NewService(ms)
	h := NewHandler(svc, nil, nil, nil)

	mux := http.NewServeMux()
	mux.HandleFunc("/games/", h.GameByID)
	wrapped := middleware.LoggingMiddleware(nil, nil, mux)

	req := httptest.NewRequest(http.MethodGet, "/games/missing", nil)
	req.Header.Set("X-Request-ID", "abc123")
	rr := httptest.NewRecorder()

	wrapped.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}

	var resp map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp["requestId"] != "abc123" {
		t.Fatalf("expected requestId propagated, got %s", resp["requestId"])
	}
	if resp["error"] == "" {
		t.Fatalf("expected error field in response")
	}
}

func TestReady(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := games.NewService(ms)
	h := NewHandler(svc, nil, nil, nil)

	req := httptest.NewRequest("GET", "/ready", nil)
	rr := httptest.NewRecorder()

	h.Ready(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestReadyWithStatus(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := games.NewService(ms)
	h := NewHandler(svc, nil, nil, func() poller.Status {
		return poller.Status{
			LastSuccess: time.Now(),
		}
	})

	req := httptest.NewRequest("GET", "/ready", nil)
	rr := httptest.NewRecorder()

	h.Ready(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestReadyNotReady(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := games.NewService(ms)
	h := NewHandler(svc, nil, nil, func() poller.Status {
		return poller.Status{}
	})

	req := httptest.NewRequest("GET", "/ready", nil)
	rr := httptest.NewRecorder()

	h.Ready(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
}

func TestGamesTodayHonorsTimezone(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := games.NewService(ms)
	h := NewHandler(svc, nil, nil, nil)

	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("failed to load location: %v", err)
	}
	h.now = func() time.Time {
		return time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC).In(loc)
	}

	req := httptest.NewRequest("GET", "/games/today?tz=America/New_York", nil)
	rr := httptest.NewRecorder()

	h.GamesToday(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	var resp domain.TodayResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed decoding games response: %v", err)
	}

	if resp.Date != "2024-01-01" {
		t.Fatalf("expected date 2024-01-01 for America/New_York, got %s", resp.Date)
	}
}

func TestGamesTodayLogsCacheHitsWhenNoDateParam(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := games.NewService(ms)
	h := NewHandler(svc, nil, nil, nil)

	req := httptest.NewRequest("GET", "/games", nil)
	rr := httptest.NewRecorder()

	h.GamesToday(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestGamesTodayUpstreamErrorsReturnBadGateway(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := games.NewService(ms)

	provider := &stubProvider{
		err: errors.New("boom"),
	}

	h := NewHandler(svc, nil, provider, nil)

	req := httptest.NewRequest("GET", "/games?date=2024-02-01", nil)
	rr := httptest.NewRecorder()

	h.GamesToday(rr, req)

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d", rr.Code)
	}
}
