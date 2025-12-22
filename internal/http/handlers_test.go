package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"nba-games-service/internal/domain"
	"nba-games-service/internal/poller"
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
	svc := domain.NewService(ms)
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
	svc := domain.NewService(ms)

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
	svc := domain.NewService(ms)
	h := NewHandler(svc, nil, &stubProvider{}, nil)

	req := httptest.NewRequest("GET", "/games?date=not-a-date", nil)
	rr := httptest.NewRecorder()

	h.GamesToday(rr, req)

	if rr.Code != 400 {
		t.Fatalf("expected 400, got %d", rr.Code)
	}

	var resp map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed decoding error response: %v", err)
	}
	if resp["error"] == "" {
		t.Fatalf("expected error message")
	}
}

func TestGamesTodayUpstreamFailureReturnsStandardError(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := domain.NewService(ms)

	tests := []struct {
		name          string
		err           error
		expectedError string
		withReqID     bool
	}{
		{
			name:          "generic",
			err:           errors.New("boom"),
			expectedError: "upstream temporarily unavailable",
			withReqID:     true,
		},
		{
			name:          "deadline",
			err:           context.DeadlineExceeded,
			expectedError: "upstream timed out",
			withReqID:     false,
		},
	}

	for _, tc := range tests {
		provider := &stubProvider{err: tc.err}
		h := NewHandler(svc, nil, provider, nil)

		req := httptest.NewRequest("GET", "/games?date=2024-02-01", nil)
		if tc.withReqID {
			req = req.WithContext(withRequestID(req.Context(), "req-123"))
		}
		rr := httptest.NewRecorder()

		h.GamesToday(rr, req)

		if rr.Code != 502 {
			t.Fatalf("%s: expected 502, got %d", tc.name, rr.Code)
		}

		var resp map[string]string
		if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
			t.Fatalf("%s: failed decoding error response: %v", tc.name, err)
		}
		if resp["error"] != tc.expectedError {
			t.Fatalf("%s: unexpected error message %q", tc.name, resp["error"])
		}
		_, hasReqID := resp["requestId"]
		if tc.withReqID && !hasReqID {
			t.Fatalf("%s: expected requestId to be included", tc.name)
		}
		if !tc.withReqID && hasReqID {
			t.Fatalf("%s: expected requestId to be absent", tc.name)
		}
	}
}

func TestGameByIDNotFound(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := domain.NewService(ms)
	h := NewHandler(svc, nil, nil, nil)

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
	h := NewHandler(svc, nil, nil, nil)

	cases := []struct {
		name string
		path string
	}{
		{"missing", "/games/"},
		{"empty", "/games"},
		{"whitespace", "/games/%20bad"},
	}

	for _, c := range cases {
		req := httptest.NewRequest("GET", c.path, nil)
		rr := httptest.NewRecorder()

		h.GameByID(rr, req)

		if rr.Code != 400 {
			t.Fatalf("%s: expected 400, got %d", c.name, rr.Code)
		}
		var resp map[string]string
		if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
			t.Fatalf("%s: failed to decode response: %v", c.name, err)
		}
		if resp["error"] == "" {
			t.Fatalf("%s: expected error message", c.name)
		}
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

	h := NewHandler(svc, nil, nil, nil)

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

func TestHandlersRejectNonGET(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := domain.NewService(ms)
	h := NewHandler(svc, nil, nil, nil)
	router := NewRouter(h)

	cases := []string{
		"/health",
		"/ready",
		"/games",
		"/games/today",
		"/games/game-1",
	}

	for _, path := range cases {
		req := httptest.NewRequest("POST", path, nil)
		rr := httptest.NewRecorder()

		router.ServeHTTP(rr, req)

		if rr.Code != 405 {
			t.Fatalf("%s: expected 405, got %d", path, rr.Code)
		}
	}
}

func TestReadyNotReadyReturns503(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := domain.NewService(ms)
	h := NewHandler(svc, nil, nil, func() poller.Status {
		return poller.Status{
			LastError:           "not ready",
			ConsecutiveFailures: 3,
		}
	})

	req := httptest.NewRequest("GET", "/ready", nil)
	rr := httptest.NewRecorder()

	h.Ready(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
}

func TestReadyReturnsOKWhenReady(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := domain.NewService(ms)
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

func TestReadyWithNilStatusFnDefaultsReady(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := domain.NewService(ms)
	h := NewHandler(svc, nil, nil, nil)

	req := httptest.NewRequest("GET", "/ready", nil)
	rr := httptest.NewRecorder()

	h.Ready(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestReadyNotReadyDefaultMessage(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := domain.NewService(ms)
	h := NewHandler(svc, nil, nil, func() poller.Status {
		return poller.Status{ConsecutiveFailures: 5}
	})

	req := httptest.NewRequest("GET", "/ready", nil)
	rr := httptest.NewRecorder()

	h.Ready(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rr.Code)
	}
}
