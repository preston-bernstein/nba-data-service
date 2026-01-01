package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"nba-data-service/internal/app/games"
	"nba-data-service/internal/domain"
	"nba-data-service/internal/snapshots"
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

func TestAdminRefreshRequiresAuth(t *testing.T) {
	h := NewAdminHandler(nil, nil, nil, "secret", nil)
	req := httptest.NewRequest(http.MethodPost, "/admin/snapshots/refresh", nil)
	rr := httptest.NewRecorder()

	h.RefreshSnapshots(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestAdminRefreshWritesSnapshot(t *testing.T) {
	app := games.NewService(nil)
	writer := snapshots.NewWriter(t.TempDir(), 1)
	provider := &stubProvider{
		games: []domain.Game{{ID: "g1"}},
	}
	h := NewAdminHandler(app, writer, provider, "secret", nil)

	req := httptest.NewRequest(http.MethodPost, "/admin/snapshots/refresh?date=2024-01-01", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rr := httptest.NewRecorder()

	h.RefreshSnapshots(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestAdminRefreshRejectsInvalidDate(t *testing.T) {
	app := games.NewService(nil)
	writer := snapshots.NewWriter(t.TempDir(), 1)
	provider := &stubProvider{
		games: []domain.Game{{ID: "g1"}},
	}
	h := NewAdminHandler(app, writer, provider, "secret", nil)

	req := httptest.NewRequest(http.MethodPost, "/admin/snapshots/refresh?date=bad-date", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rr := httptest.NewRecorder()

	h.RefreshSnapshots(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}
