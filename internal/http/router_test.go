package http

import (
	"net/http"
	"testing"

	"nba-data-service/internal/app/games"
	"nba-data-service/internal/http/handlers"
	"nba-data-service/internal/store"
	"nba-data-service/internal/testutil"
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
		rr := testutil.Serve(router, http.MethodGet, path, nil)
		testutil.AssertStatus(t, rr, expected)
	}
}

func TestRouterUnknownRouteReturns404(t *testing.T) {
	ms := store.NewMemoryStore()
	svc := games.NewService(ms)
	h := handlers.NewHandler(svc, nil, nil, nil)

	router := NewRouter(h)

	rr := testutil.Serve(router, http.MethodGet, "/does-not-exist", nil)
	testutil.AssertStatus(t, rr, http.StatusNotFound)
}
