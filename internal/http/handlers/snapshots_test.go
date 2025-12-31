package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"nba-data-service/internal/app/games"
	"nba-data-service/internal/domain"
	"nba-data-service/internal/store"
)

func TestGamesTodayFallsBackToSnapshotWhenCacheEmpty(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := games.NewService(ms)
	snaps := &stubSnapshots{
		resp: domain.TodayResponse{
			Date:  "2024-02-01",
			Games: []domain.Game{{ID: "snapshot-game"}},
		},
	}

	h := NewHandler(svc, snaps, nil, nil)
	h.now = func() time.Time { return time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC) }

	req := httptest.NewRequest(http.MethodGet, "/games/today", nil)
	rr := httptest.NewRecorder()

	h.GamesToday(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	var resp domain.TodayResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Date != "2024-02-01" || len(resp.Games) != 1 || resp.Games[0].ID != "snapshot-game" {
		t.Fatalf("unexpected snapshot response %+v", resp)
	}
}
