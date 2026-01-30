package http

import (
	"net/http"
	"testing"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/http/handlers"
	"github.com/preston-bernstein/nba-data-service/internal/teststubs"
	"github.com/preston-bernstein/nba-data-service/internal/testutil"
)

func TestRouterRoutesKnownPaths(t *testing.T) {
	snaps := &teststubs.StubSnapshotStore{}
	h := handlers.NewHandler(snaps, nil, nil)

	router := NewRouter(h)

	cases := map[string]int{
		"/health":      http.StatusOK,
		"/games":       http.StatusBadRequest,
		"/games/today": http.StatusNotFound,
		"/games/foo":   http.StatusNotFound, // known route with missing game
	}

	for path, expected := range cases {
		rr := testutil.Serve(router, http.MethodGet, path, nil)
		testutil.AssertStatus(t, rr, expected)
	}
}

func TestRouterUnknownRouteReturns404(t *testing.T) {
	snaps := &teststubs.StubSnapshotStore{}
	h := handlers.NewHandler(snaps, nil, nil)

	router := NewRouter(h)

	rr := testutil.Serve(router, http.MethodGet, "/does-not-exist", nil)
	testutil.AssertStatus(t, rr, http.StatusNotFound)
}

// StubSnapshotStore is defined in teststubs for reuse across packages.
var _ interface {
	LoadGames(date string) (domaingames.TodayResponse, error)
	FindGameByID(date, id string) (domaingames.Game, bool)
} = (*teststubs.StubSnapshotStore)(nil)
