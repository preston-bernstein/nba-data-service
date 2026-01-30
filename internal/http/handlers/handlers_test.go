package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
	"github.com/preston-bernstein/nba-data-service/internal/http/middleware"
	"github.com/preston-bernstein/nba-data-service/internal/logging"
	"github.com/preston-bernstein/nba-data-service/internal/poller"
	"github.com/preston-bernstein/nba-data-service/internal/snapshots"
	"github.com/preston-bernstein/nba-data-service/internal/teststubs"
	"github.com/preston-bernstein/nba-data-service/internal/testutil"
)

func newHandler(snaps snapshots.Store, statusFn func() poller.Status) *Handler {
	return NewHandler(snaps, nil, statusFn)
}

func storeWithResponse(date string, resp domaingames.TodayResponse) *teststubs.StubSnapshotStore {
	return &teststubs.StubSnapshotStore{
		Games: map[string]domaingames.TodayResponse{
			date: resp,
		},
	}
}

func storeWithGames(date string, games []domaingames.Game) *teststubs.StubSnapshotStore {
	return storeWithResponse(date, domaingames.NewTodayResponse(date, games))
}

func TestHealth(t *testing.T) {
	h := newHandler(nil, nil)

	rr := testutil.Serve(h, http.MethodGet, "/health", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)

	var resp map[string]string
	testutil.DecodeJSON(t, rr, &resp)
	if resp["status"] != "ok" {
		t.Fatalf("expected status ok, got %s", resp["status"])
	}
}

func TestHealthShuttingDownReturnsServiceUnavailable(t *testing.T) {
	h := newHandler(nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	ctx, cancel := context.WithCancel(req.Context())
	cancel()
	req = req.WithContext(ctx)
	rr := testutil.ServeRequest(http.HandlerFunc(h.Health), req)

	testutil.AssertStatus(t, rr, http.StatusServiceUnavailable)
	var resp map[string]string
	testutil.DecodeJSON(t, rr, &resp)
	if resp["error"] != "shutting down" {
		t.Fatalf("unexpected error %q", resp["error"])
	}
}
func TestGamesByDateRequiresDate(t *testing.T) {
	h := newHandler(nil, nil)

	rr := testutil.Serve(h, http.MethodGet, "/games", nil)
	testutil.AssertStatus(t, rr, http.StatusBadRequest)
}

func TestGamesByDateUsesSnapshot(t *testing.T) {
	date := "2024-02-01"
	snaps := storeWithResponse(date, testutil.SampleTodayResponse(date, "snapshot-game"))

	h := newHandler(snaps, nil)
	h.now = func() time.Time { return time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC) }

	rr := testutil.Serve(h, http.MethodGet, "/games?date=2024-02-01", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)

	var resp domaingames.TodayResponse
	testutil.DecodeJSON(t, rr, &resp)

	if resp.Date != "2024-02-01" {
		t.Fatalf("expected date to reflect query param, got %s", resp.Date)
	}
	if len(resp.Games) != 1 || resp.Games[0].ID != "snapshot-game" {
		t.Fatalf("expected provider games, got %+v", resp.Games)
	}
}

func TestGamesByDateWithInvalidDateReturnsBadRequest(t *testing.T) {
	h := newHandler(nil, nil)

	rr := testutil.Serve(h, http.MethodGet, "/games?date=not-a-date", nil)
	testutil.AssertStatus(t, rr, http.StatusBadRequest)
}

func TestGamesByDateOutOfRangeReturnsBadRequest(t *testing.T) {
	h := newHandler(nil, nil)
	h.now = func() time.Time { return time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC) }

	// 8 days before - should fail
	rr := testutil.Serve(h, http.MethodGet, "/games?date=2024-01-06", nil)
	testutil.AssertStatus(t, rr, http.StatusBadRequest)

	// 8 days after - should fail
	rr = testutil.Serve(h, http.MethodGet, "/games?date=2024-01-24", nil)
	testutil.AssertStatus(t, rr, http.StatusBadRequest)

	// 7 days before - should pass (with snapshot error, but not bad request)
	rr = testutil.Serve(h, http.MethodGet, "/games?date=2024-01-08", nil)
	if rr.Code == http.StatusBadRequest {
		t.Fatalf("expected date within range to not return bad request")
	}

	// 7 days after - should pass
	rr = testutil.Serve(h, http.MethodGet, "/games?date=2024-01-22", nil)
	if rr.Code == http.StatusBadRequest {
		t.Fatalf("expected date within range to not return bad request")
	}
}

func TestGamesByDateLogsSnapshotErrors(t *testing.T) {
	snaps := &teststubs.StubSnapshotStore{LoadErr: errors.New("missing snapshot")}

	h := newHandler(snaps, nil)
	h.now = func() time.Time { return time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC) }

	rr := testutil.Serve(h, http.MethodGet, "/games?date=2024-02-01", nil)
	testutil.AssertStatus(t, rr, http.StatusBadGateway)
}

func TestGamesByDateSnapshotMissingReturnsBadGateway(t *testing.T) {
	h := newHandler(nil, nil)
	h.now = func() time.Time { return time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC) }

	rr := testutil.Serve(h, http.MethodGet, "/games?date=2024-04-01", nil)

	testutil.AssertStatus(t, rr, http.StatusBadGateway)
}

func TestGamesByDateWithNilSnapshotsReturnsBadGateway(t *testing.T) {
	h := newHandler(nil, nil)
	h.now = func() time.Time { return time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC) }
	rr := testutil.Serve(h, http.MethodGet, "/games?date=2024-06-01", nil)
	testutil.AssertStatus(t, rr, http.StatusBadGateway)
}

func TestGamesByDateWithLoggerLogsSnapshot(t *testing.T) {
	logger, buf := testutil.NewBufferLogger()
	date := "2024-07-01"
	snaps := storeWithResponse(date, testutil.SampleTodayResponse(date, "logged-snap"))
	h := NewHandler(snaps, logger, nil)
	h.now = func() time.Time { return time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC) }
	rr := testutil.Serve(h, http.MethodGet, "/games?date=2024-07-01", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)
	if buf.Len() == 0 {
		t.Fatalf("expected snapshot log when logger provided")
	}
}

func TestGameByID(t *testing.T) {
	date := "2024-01-01"
	game := testutil.SampleGame("id-1")
	game.StartTime = time.Date(2024, 1, 1, 15, 30, 0, 0, time.UTC).Format(time.RFC3339)
	snaps := storeWithGames(date, []domaingames.Game{game})
	h := newHandler(snaps, nil)
	h.now = func() time.Time { return time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC) }

	rr := testutil.Serve(http.HandlerFunc(h.GameByID), http.MethodGet, "/games/id-1", nil)

	testutil.AssertStatus(t, rr, http.StatusOK)

	var resp domaingames.Game
	testutil.DecodeJSON(t, rr, &resp)

	if resp.ID != "id-1" {
		t.Fatalf("expected game id id-1, got %s", resp.ID)
	}
}

func TestGameByIDInvalid(t *testing.T) {
	h := newHandler(nil, nil)

	rr := testutil.Serve(http.HandlerFunc(h.GameByID), http.MethodGet, "/games", nil)

	testutil.AssertStatus(t, rr, http.StatusBadRequest)
}

func TestGameByIDNotFound(t *testing.T) {
	date := "2024-01-01"
	snaps := storeWithGames(date, nil)
	h := newHandler(snaps, nil)
	h.now = func() time.Time { return time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC) }

	rr := testutil.Serve(http.HandlerFunc(h.GameByID), http.MethodGet, "/games/unknown", nil)

	testutil.AssertStatus(t, rr, http.StatusNotFound)
}

func TestGameByIDInvalidCharacters(t *testing.T) {
	h := newHandler(nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/games/bad%20id", nil)
	rr := testutil.ServeRequest(http.HandlerFunc(h.GameByID), req)

	testutil.AssertStatus(t, rr, http.StatusBadRequest)
}

func TestGameByIDWithEncodedSlash(t *testing.T) {
	h := newHandler(nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/games/foo%2Fbar", nil)
	rr := testutil.ServeRequest(http.HandlerFunc(h.GameByID), req)

	testutil.AssertStatus(t, rr, http.StatusBadRequest)
}

func TestGameByIDUnescapeError(t *testing.T) {
	h := newHandler(nil, nil)

	req := &http.Request{
		Method: http.MethodGet,
		URL: &url.URL{
			Path:    "/games/%zz",
			RawPath: "/games/%zz",
		},
		Header: make(http.Header),
	}
	rr := testutil.ServeRequest(http.HandlerFunc(h.GameByID), req)

	testutil.AssertStatus(t, rr, http.StatusBadRequest)
}

func TestGameByIDRejectsGamesKeyword(t *testing.T) {
	h := newHandler(nil, nil)
	rr := testutil.Serve(h, http.MethodGet, "/games/games", nil)
	testutil.AssertStatus(t, rr, http.StatusBadRequest)
}

func TestGameByIDNilSnapshotStore(t *testing.T) {
	h := NewHandler(nil, nil, nil)
	rr := testutil.Serve(http.HandlerFunc(h.GameByID), http.MethodGet, "/games/id-1", nil)
	testutil.AssertStatus(t, rr, http.StatusBadGateway)
}

func TestGamesByDateReturnsSnapshot(t *testing.T) {
	date := "2024-03-01"
	snaps := storeWithResponse(date, testutil.SampleTodayResponse(date, "snap-id"))
	h := newHandler(snaps, nil)
	h.now = func() time.Time { return time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC) }

	rr := testutil.Serve(h, http.MethodGet, "/games?date=2024-03-01", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)

	var resp domaingames.TodayResponse
	testutil.DecodeJSON(t, rr, &resp)
	if resp.Date != "2024-03-01" || len(resp.Games) != 1 || resp.Games[0].ID != "snap-id" {
		t.Fatalf("expected snapshot response, got %+v", resp)
	}
}

func TestMethodNotAllowedHandlers(t *testing.T) {
	h := newHandler(nil, nil)

	tests := []struct {
		name string
		path string
		fn   func(w http.ResponseWriter, r *http.Request)
	}{
		{"health", "/health", h.Health},
		{"ready", "/ready", h.Ready},
		{"gamesByDate", "/games?date=2024-01-01", h.GamesToday},
		{"gameByID", "/games/id", h.GameByID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr := testutil.Serve(http.HandlerFunc(tt.fn), http.MethodPost, tt.path, nil)
			testutil.AssertStatus(t, rr, http.StatusMethodNotAllowed)
		})
	}
}

func TestRequestIDPropagatesThroughMiddleware(t *testing.T) {
	date := "2024-01-01"
	snaps := storeWithGames(date, nil)
	h := newHandler(snaps, nil)
	h.now = func() time.Time { return time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC) }

	mux := http.NewServeMux()
	mux.HandleFunc("/games/", h.GameByID)
	wrapped := middleware.LoggingMiddleware(nil, nil, mux)

	req := httptest.NewRequest(http.MethodGet, "/games/missing", nil)
	req.Header.Set("X-Request-ID", "abc123")
	rr := testutil.ServeRequest(wrapped, req)

	testutil.AssertStatus(t, rr, http.StatusNotFound)

	var resp map[string]string
	testutil.DecodeJSON(t, rr, &resp)
	if resp["requestId"] != "abc123" {
		t.Fatalf("expected requestId propagated, got %s", resp["requestId"])
	}
	if resp["error"] == "" {
		t.Fatalf("expected error field in response")
	}
}

func TestReady(t *testing.T) {
	h := newHandler(nil, nil)

	rr := testutil.Serve(http.HandlerFunc(h.Ready), http.MethodGet, "/ready", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)
}

func TestReadyWithStatus(t *testing.T) {
	h := newHandler(nil, func() poller.Status {
		return poller.Status{
			LastSuccess: time.Now(),
		}
	})

	rr := testutil.Serve(http.HandlerFunc(h.Ready), http.MethodGet, "/ready", nil)

	testutil.AssertStatus(t, rr, http.StatusOK)
}

func TestReadyNotReady(t *testing.T) {
	h := newHandler(nil, func() poller.Status {
		return poller.Status{}
	})

	rr := testutil.Serve(http.HandlerFunc(h.Ready), http.MethodGet, "/ready", nil)

	testutil.AssertStatus(t, rr, http.StatusServiceUnavailable)
}

func TestReadyNotReadyUsesLastError(t *testing.T) {
	h := newHandler(nil, func() poller.Status {
		return poller.Status{
			LastError: "upstream down",
		}
	})

	rr := testutil.Serve(http.HandlerFunc(h.Ready), http.MethodGet, "/ready", nil)

	testutil.AssertStatus(t, rr, http.StatusServiceUnavailable)
	var resp map[string]string
	testutil.DecodeJSON(t, rr, &resp)
	if resp["error"] != "upstream down" {
		t.Fatalf("expected last error propagated, got %s", resp["error"])
	}
}

func TestServeHTTPNotFound(t *testing.T) {
	h := newHandler(nil, nil)
	rr := testutil.Serve(h, http.MethodGet, "/unknown", nil)
	testutil.AssertStatus(t, rr, http.StatusNotFound)
}

// Routing via ServeHTTP (switch on path)
func TestServeHTTPRoutes(t *testing.T) {
	date := "2024-01-01"
	game := testutil.SampleGame("id-1")
	game.StartTime = time.Now().Format(time.RFC3339)
	snaps := storeWithGames(date, []domaingames.Game{game})
	h := NewHandler(snaps, nil, func() poller.Status { return poller.Status{LastSuccess: time.Now()} })
	h.now = func() time.Time { return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) }

	tests := []struct {
		path   string
		status int
	}{
		{"/health", http.StatusOK},
		{"/ready", http.StatusOK},
		{"/games?date=2024-01-01", http.StatusOK},
		{"/games/id-1", http.StatusOK},
	}

	for _, tt := range tests {
		rr := testutil.Serve(h, http.MethodGet, tt.path, nil)
		testutil.AssertStatus(t, rr, tt.status)
	}
}

func TestServeHTTPRoutingAndMethods(t *testing.T) {
	date := "2024-01-01"
	snaps := storeWithGames(date, nil)
	h := newHandler(snaps, nil)
	h.now = func() time.Time { return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC) }
	tests := []struct {
		method string
		path   string
		status int
	}{
		{http.MethodPost, "/health", http.StatusMethodNotAllowed},
		{http.MethodPost, "/ready", http.StatusMethodNotAllowed},
		{http.MethodPost, "/games?date=2024-01-01", http.StatusMethodNotAllowed},
		{http.MethodPost, "/games/id", http.StatusMethodNotAllowed},
		{http.MethodGet, "/does-not-exist", http.StatusNotFound},
		{http.MethodGet, "/games", http.StatusBadRequest},
		{http.MethodGet, "/games/", http.StatusBadRequest},
		{http.MethodGet, "/games/today", http.StatusNotFound},
		{http.MethodPost, "/unknown", http.StatusNotFound},
	}
	for _, tt := range tests {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(tt.method, tt.path, nil)
		h.ServeHTTP(rr, req)
		if rr.Code != tt.status {
			t.Fatalf("path %s method %s: expected %d, got %d", tt.path, tt.method, tt.status, rr.Code)
		}
	}
}

func TestGamesByDateUpstreamErrorsReturnBadGateway(t *testing.T) {
	snaps := &teststubs.StubSnapshotStore{LoadErr: errors.New("boom")}

	h := newHandler(snaps, nil)
	h.now = func() time.Time { return time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC) }

	rr := testutil.Serve(h, http.MethodGet, "/games?date=2024-02-01", nil)
	testutil.AssertStatus(t, rr, http.StatusBadGateway)
}

func TestRequestIDNilRequest(t *testing.T) {
	if got := requestID(nil); got != "" {
		t.Fatalf("expected empty request id for nil request, got %s", got)
	}
}

func TestLoggerFromContextPrefersAttachedLogger(t *testing.T) {
	fallback, _ := testutil.NewBufferLogger()
	ctxLogger, _ := testutil.NewBufferLogger()
	ctx := logging.WithLogger(context.Background(), ctxLogger)
	req := httptest.NewRequest(http.MethodGet, "/games?date=2024-01-01", nil).WithContext(ctx)

	got := loggerFromContext(req, fallback)
	if got != ctxLogger {
		t.Fatalf("expected logger from context to be returned")
	}
}

func TestLoggerFromContextNilRequestReturnsFallback(t *testing.T) {
	fallback, _ := testutil.NewBufferLogger()
	if got := loggerFromContext(nil, fallback); got != fallback {
		t.Fatalf("expected fallback logger when request is nil")
	}
}

// Benchmarks
func BenchmarkGamesByDate(b *testing.B) {
	now := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	basePath := b.TempDir()
	writer := snapshots.NewWriter(basePath, 1)
	if err := writer.WriteGamesSnapshot("2024-01-02", domaingames.TodayResponse{
		Date: "2024-01-02",
		Games: []domaingames.Game{
			{
				ID:        "game-1",
				Provider:  "test",
				HomeTeam:  teams.Team{ID: "home", Name: "Home"},
				AwayTeam:  teams.Team{ID: "away", Name: "Away"},
				StartTime: now.Format(time.RFC3339),
				Status:    domaingames.StatusScheduled,
				Score:     domaingames.Score{Home: 0, Away: 0},
				Meta:      domaingames.GameMeta{Season: "2023-2024", UpstreamGameID: 123},
			},
		},
	}); err != nil {
		b.Fatalf("failed to write snapshot: %v", err)
	}
	h := NewHandler(snapshots.NewFSStore(basePath), nil, nil)
	h.now = func() time.Time { return now }

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/games?date=2024-01-02", nil)
		h.GamesToday(rr, req)
	}
}

func BenchmarkGameByID(b *testing.B) {
	now := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	basePath := b.TempDir()
	writer := snapshots.NewWriter(basePath, 1)
	if err := writer.WriteGamesSnapshot("2024-01-02", domaingames.TodayResponse{
		Date: "2024-01-02",
		Games: []domaingames.Game{
			{
				ID:        "game-1",
				Provider:  "test",
				HomeTeam:  teams.Team{ID: "home", Name: "Home"},
				AwayTeam:  teams.Team{ID: "away", Name: "Away"},
				StartTime: now.Format(time.RFC3339),
				Status:    domaingames.StatusScheduled,
				Score:     domaingames.Score{Home: 0, Away: 0},
				Meta:      domaingames.GameMeta{Season: "2023-2024", UpstreamGameID: 123},
			},
		},
	}); err != nil {
		b.Fatalf("failed to write snapshot: %v", err)
	}
	h := NewHandler(snapshots.NewFSStore(basePath), nil, nil)
	h.now = func() time.Time { return now }

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/games/game-1", nil)
		h.GameByID(rr, req)
	}
}
