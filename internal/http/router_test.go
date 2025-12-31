package http

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"nba-data-service/internal/app/games"
	"nba-data-service/internal/http/handlers"
	"nba-data-service/internal/store"
)

func TestRouterRoutesKnownPaths(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := games.NewService(ms)
	h := handlers.NewHandler(svc, nil, nil, nil)

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
	svc := games.NewService(ms)
	h := handlers.NewHandler(svc, nil, nil, nil)

	router := NewRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/does-not-exist", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown route, got %d", rr.Code)
	}
}
