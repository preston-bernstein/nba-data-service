package http

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"nba-games-service/internal/domain"
	"nba-games-service/internal/store"
)

func TestRouterRoutesKnownPaths(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := domain.NewService(ms)
	h := NewHandler(svc, nil, nil, nil)

	router := NewRouter(h)

	cases := map[string]int{
		"/health":      http.StatusOK,
		"/games":       http.StatusOK,
		"/games/today": http.StatusOK,
		"/games/foo":   http.StatusNotFound, // known route with missing game
	}

	for path, expected := range cases {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != expected {
			t.Fatalf("route %s expected status %d, got %d", path, expected, rr.Code)
		}
	}
}

func TestRouterUnknownRouteReturns404(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := domain.NewService(ms)
	h := NewHandler(svc, nil, nil, nil)
	h.now = func() time.Time { return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) }

	router := NewRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/does-not-exist", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown route, got %d", rr.Code)
	}
}
