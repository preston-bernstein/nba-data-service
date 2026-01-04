package handlers

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/preston-bernstein/nba-data-service/internal/app/games"
	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
	"github.com/preston-bernstein/nba-data-service/internal/http/middleware"
	"github.com/preston-bernstein/nba-data-service/internal/logging"
	"github.com/preston-bernstein/nba-data-service/internal/poller"
	"github.com/preston-bernstein/nba-data-service/internal/snapshots"
	"github.com/preston-bernstein/nba-data-service/internal/store"
	"github.com/preston-bernstein/nba-data-service/internal/testutil"
)

type stubSnapshots struct {
	resp domaingames.TodayResponse
	err  error
}

func newHandler(g []domaingames.Game, snaps snapshots.Store, logger *slog.Logger, statusFn func() poller.Status) *Handler {
	gsvc := testutil.NewServiceWithGames(g)
	return NewHandler(gsvc, snaps, logger, statusFn)
}

func (s *stubSnapshots) LoadGames(date string) (domaingames.TodayResponse, error) {
	_ = date
	return s.resp, s.err
}

func TestHealth(t *testing.T) {
	h := newHandler(nil, nil, nil, nil)

	rr := testutil.Serve(h, http.MethodGet, "/health", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)

	var resp map[string]string
	testutil.DecodeJSON(t, rr, &resp)
	if resp["status"] != "ok" {
		t.Fatalf("expected status ok, got %s", resp["status"])
	}
}

func TestHealthShuttingDownReturnsServiceUnavailable(t *testing.T) {
	h := newHandler(nil, nil, nil, nil)

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
func TestGamesToday(t *testing.T) {
	game := testutil.SampleGame("game-1")
	game.StartTime = time.Date(2024, 1, 1, 15, 30, 0, 0, time.UTC).Format(time.RFC3339)
	h := newHandler([]domaingames.Game{game}, nil, nil, nil)
	fixedNow := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	h.now = func() time.Time { return fixedNow }

	rr := testutil.Serve(h, http.MethodGet, "/games/today", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)

	var resp domaingames.TodayResponse
	testutil.DecodeJSON(t, rr, &resp)

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
	snaps := &stubSnapshots{
		resp: testutil.SampleTodayResponse("2024-02-01", "snapshot-game"),
	}

	h := newHandler(nil, snaps, nil, nil)

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

func TestGamesTodayWithInvalidDateReturnsBadRequest(t *testing.T) {
	h := newHandler(nil, nil, nil, nil)

	rr := testutil.Serve(h, http.MethodGet, "/games?date=not-a-date", nil)
	testutil.AssertStatus(t, rr, http.StatusBadRequest)
}

func TestGamesTodayLogsUpstreamErrors(t *testing.T) {
	snaps := &stubSnapshots{
		err: errors.New("missing snapshot"),
	}

	h := newHandler(nil, snaps, nil, nil)

	rr := testutil.Serve(h, http.MethodGet, "/games?date=2024-02-01", nil)
	testutil.AssertStatus(t, rr, http.StatusBadGateway)
}

func TestGamesTodaySnapshotMissingReturnsBadGateway(t *testing.T) {
	h := newHandler(nil, nil, nil, nil)

	rr := testutil.Serve(h, http.MethodGet, "/games?date=2024-04-01", nil)

	testutil.AssertStatus(t, rr, http.StatusBadGateway)
}

func TestGamesTodayCacheEmptyLoadsSnapshotAndLogs(t *testing.T) {
	snaps := &stubSnapshots{
		resp: testutil.SampleTodayResponse("2024-05-01", "snap-cache"),
	}
	logger, buf := testutil.NewBufferLogger()
	h := newHandler(nil, snaps, logger, nil)

	rr := testutil.Serve(h, http.MethodGet, "/games/today", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)

	var resp domaingames.TodayResponse
	testutil.DecodeJSON(t, rr, &resp)
	if resp.Date != "2024-05-01" || resp.Games[0].ID != "snap-cache" {
		t.Fatalf("expected snapshot fallback, got %+v", resp)
	}
	if buf.Len() == 0 {
		t.Fatalf("expected logging when serving snapshot")
	}
}

func TestGamesTodayDateWithNilSnapshotsReturnsBadGateway(t *testing.T) {
	h := newHandler(nil, nil, nil, nil)
	rr := testutil.Serve(h, http.MethodGet, "/games?date=2024-06-01", nil)
	testutil.AssertStatus(t, rr, http.StatusBadGateway)
}

func TestGamesTodayRejectsInvalidTimezoneAndReturnsOKWithDefault(t *testing.T) {
	h := newHandler(nil, nil, nil, nil)
	rr := testutil.Serve(h, http.MethodGet, "/games/today?tz=Bad/Timezone", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)
}

func TestGamesTodayValidTimezoneAdjustsDate(t *testing.T) {
	h := newHandler(nil, nil, nil, nil)
	loc, _ := time.LoadLocation("America/New_York")
	h.now = func() time.Time { return time.Date(2024, 3, 2, 2, 0, 0, 0, loc) } // should map to 2024-03-02 local
	rr := testutil.Serve(h, http.MethodGet, "/games/today?tz=America/New_York", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)
	var resp domaingames.TodayResponse
	testutil.DecodeJSON(t, rr, &resp)
	if resp.Date != "2024-03-02" {
		t.Fatalf("expected tz-adjusted date, got %s", resp.Date)
	}
}

func TestGamesTodayServesCachedGamesLogs(t *testing.T) {
	logger, buf := testutil.NewBufferLogger()
	game := testutil.SampleGame("cached-1")
	game.StartTime = time.Now().Format(time.RFC3339)
	h := newHandler([]domaingames.Game{game}, nil, logger, nil)
	rr := testutil.Serve(h, http.MethodGet, "/games/today", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)
	if buf.Len() == 0 {
		t.Fatalf("expected log entry for cached games")
	}
}

func TestGamesTodaySnapshotErrorFallsBackToEmptyCache(t *testing.T) {
	logger, _ := testutil.NewBufferLogger()
	h := newHandler(nil, &stubSnapshots{err: errors.New("boom")}, logger, nil)
	rr := testutil.Serve(h, http.MethodGet, "/games/today", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)
	var resp domaingames.TodayResponse
	testutil.DecodeJSON(t, rr, &resp)
	if resp.Date == "" {
		t.Fatalf("expected date to be set")
	}
	if len(resp.Games) != 0 {
		t.Fatalf("expected empty games when snapshot fetch fails, got %d", len(resp.Games))
	}
}

func TestGamesTodayDateWithLoggerLogsSnapshot(t *testing.T) {
	logger, buf := testutil.NewBufferLogger()
	snaps := &stubSnapshots{resp: testutil.SampleTodayResponse("2024-07-01", "logged-snap")}
	h := newHandler(nil, snaps, logger, nil)
	rr := testutil.Serve(h, http.MethodGet, "/games?date=2024-07-01", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)
	if buf.Len() == 0 {
		t.Fatalf("expected snapshot log when logger provided")
	}
}

func TestGamesTodayInvalidTimezoneFallsBack(t *testing.T) {
	h := newHandler(nil, nil, nil, nil)

	fixedNow := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	h.now = func() time.Time { return fixedNow }

	rr := testutil.Serve(h, http.MethodGet, "/games/today?tz=invalid-timezone", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)

	var resp domaingames.TodayResponse
	testutil.DecodeJSON(t, rr, &resp)

	if resp.Date != "2024-01-02" {
		t.Fatalf("expected date 2024-01-02, got %s", resp.Date)
	}
}

func TestGameByID(t *testing.T) {
	game := testutil.SampleGame("id-1")
	game.StartTime = time.Date(2024, 1, 1, 15, 30, 0, 0, time.UTC).Format(time.RFC3339)
	h := newHandler([]domaingames.Game{game}, nil, nil, nil)

	rr := testutil.Serve(http.HandlerFunc(h.GameByID), http.MethodGet, "/games/id-1", nil)

	testutil.AssertStatus(t, rr, http.StatusOK)

	var resp domaingames.Game
	testutil.DecodeJSON(t, rr, &resp)

	if resp.ID != "id-1" {
		t.Fatalf("expected game id id-1, got %s", resp.ID)
	}
}

func TestGameByIDInvalid(t *testing.T) {
	h := newHandler(nil, nil, nil, nil)

	rr := testutil.Serve(http.HandlerFunc(h.GameByID), http.MethodGet, "/games", nil)

	testutil.AssertStatus(t, rr, http.StatusBadRequest)
}

func TestGameByIDNotFound(t *testing.T) {
	h := newHandler(nil, nil, nil, nil)

	rr := testutil.Serve(http.HandlerFunc(h.GameByID), http.MethodGet, "/games/unknown", nil)

	testutil.AssertStatus(t, rr, http.StatusNotFound)
}

func TestGameByIDInvalidCharacters(t *testing.T) {
	h := newHandler(nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/games/bad%20id", nil)
	rr := testutil.ServeRequest(http.HandlerFunc(h.GameByID), req)

	testutil.AssertStatus(t, rr, http.StatusBadRequest)
}

func TestGameByIDWithEncodedSlash(t *testing.T) {
	h := newHandler(nil, nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/games/foo%2Fbar", nil)
	rr := testutil.ServeRequest(http.HandlerFunc(h.GameByID), req)

	testutil.AssertStatus(t, rr, http.StatusBadRequest)
}

func TestGameByIDUnescapeError(t *testing.T) {
	h := newHandler(nil, nil, nil, nil)

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
	h := newHandler(nil, nil, nil, nil)
	rr := testutil.Serve(h, http.MethodGet, "/games/games", nil)
	testutil.AssertStatus(t, rr, http.StatusBadRequest)
}

func TestGamesTodayExplicitDateFallsBackToSnapshotWhenEmptyCache(t *testing.T) {
	snaps := &stubSnapshots{
		resp: testutil.SampleTodayResponse("2024-03-01", "snap-id"),
	}
	h := newHandler(nil, snaps, nil, nil)

	rr := testutil.Serve(h, http.MethodGet, "/games?date=2024-03-01", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)

	var resp domaingames.TodayResponse
	testutil.DecodeJSON(t, rr, &resp)
	if resp.Date != "2024-03-01" || len(resp.Games) != 1 || resp.Games[0].ID != "snap-id" {
		t.Fatalf("expected snapshot response, got %+v", resp)
	}
}

func TestMethodNotAllowedHandlers(t *testing.T) {
	h := newHandler(nil, nil, nil, nil)

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
			rr := testutil.Serve(http.HandlerFunc(tt.fn), http.MethodPost, tt.path, nil)
			testutil.AssertStatus(t, rr, http.StatusMethodNotAllowed)
		})
	}
}

func TestRequestIDPropagatesThroughMiddleware(t *testing.T) {
	h := newHandler(nil, nil, nil, nil)

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
	h := newHandler(nil, nil, nil, nil)

	rr := testutil.Serve(http.HandlerFunc(h.Ready), http.MethodGet, "/ready", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)
}

func TestReadyWithStatus(t *testing.T) {
	h := newHandler(nil, nil, nil, func() poller.Status {
		return poller.Status{
			LastSuccess: time.Now(),
		}
	})

	rr := testutil.Serve(http.HandlerFunc(h.Ready), http.MethodGet, "/ready", nil)

	testutil.AssertStatus(t, rr, http.StatusOK)
}

func TestReadyNotReady(t *testing.T) {
	h := newHandler(nil, nil, nil, func() poller.Status {
		return poller.Status{}
	})

	rr := testutil.Serve(http.HandlerFunc(h.Ready), http.MethodGet, "/ready", nil)

	testutil.AssertStatus(t, rr, http.StatusServiceUnavailable)
}

func TestReadyNotReadyUsesLastError(t *testing.T) {
	h := newHandler(nil, nil, nil, func() poller.Status {
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

func TestGamesTodayHonorsTimezone(t *testing.T) {
	h := newHandler(nil, nil, nil, nil)

	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("failed to load location: %v", err)
	}
	h.now = func() time.Time {
		return time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC).In(loc)
	}

	rr := testutil.Serve(h, http.MethodGet, "/games/today?tz=America/New_York", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)

	var resp domaingames.TodayResponse
	testutil.DecodeJSON(t, rr, &resp)

	if resp.Date != "2024-01-01" {
		t.Fatalf("expected date 2024-01-01 for America/New_York, got %s", resp.Date)
	}
}

func TestGamesTodayLogsCacheHitsWhenNoDateParam(t *testing.T) {
	logger, buf := testutil.NewBufferLogger()
	h := newHandler(nil, nil, logger, nil)

	rr := testutil.Serve(h, http.MethodGet, "/games", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)
	if buf.Len() == 0 {
		t.Fatalf("expected cache log entry")
	}
}

func TestGamesTodayLogsCacheWhenSnapshotMissing(t *testing.T) {
	logger, buf := testutil.NewBufferLogger()
	h := newHandler(nil, &stubSnapshots{err: errors.New("no snapshot")}, logger, nil)

	rr := testutil.Serve(h, http.MethodGet, "/games/today", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)

	var resp domaingames.TodayResponse
	testutil.DecodeJSON(t, rr, &resp)
	if resp.Date == "" {
		t.Fatalf("expected a computed date")
	}
	if buf.Len() == 0 {
		t.Fatalf("expected cache log entry even when snapshot missing")
	}
}

func TestServeHTTPNotFound(t *testing.T) {
	h := newHandler(nil, nil, nil, nil)
	rr := testutil.Serve(h, http.MethodGet, "/unknown", nil)
	testutil.AssertStatus(t, rr, http.StatusNotFound)
}

// Routing via ServeHTTP (switch on path)
func TestServeHTTPRoutes(t *testing.T) {
	game := testutil.SampleGame("id-1")
	game.StartTime = time.Now().Format(time.RFC3339)
	svc := testutil.NewServiceWithGames([]domaingames.Game{game})
	h := NewHandler(svc, &stubSnapshots{resp: domaingames.TodayResponse{Date: "2024-01-01"}}, nil, func() poller.Status { return poller.Status{LastSuccess: time.Now()} })

	tests := []struct {
		path   string
		status int
	}{
		{"/health", http.StatusOK},
		{"/ready", http.StatusOK},
		{"/games/today", http.StatusOK},
		{"/games/id-1", http.StatusOK},
	}

	for _, tt := range tests {
		rr := testutil.Serve(h, http.MethodGet, tt.path, nil)
		testutil.AssertStatus(t, rr, tt.status)
	}
}

func TestServeHTTPRoutingAndMethods(t *testing.T) {
	h := newHandler(nil, nil, nil, nil)
	tests := []struct {
		method string
		path   string
		status int
	}{
		{http.MethodPost, "/health", http.StatusMethodNotAllowed},
		{http.MethodPost, "/ready", http.StatusMethodNotAllowed},
		{http.MethodPost, "/games/today", http.StatusMethodNotAllowed},
		{http.MethodPost, "/games/id", http.StatusMethodNotAllowed},
		{http.MethodGet, "/does-not-exist", http.StatusNotFound},
		{http.MethodGet, "/games/", http.StatusBadRequest},
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

func TestGamesTodayUpstreamErrorsReturnBadGateway(t *testing.T) {
	snaps := &stubSnapshots{
		err: errors.New("boom"),
	}

	h := newHandler(nil, snaps, nil, nil)

	rr := testutil.Serve(h, http.MethodGet, "/games?date=2024-02-01", nil)
	testutil.AssertStatus(t, rr, http.StatusBadGateway)
}

func TestGamesTodayFallsBackToSnapshotWhenCacheEmpty(t *testing.T) {
	snaps := &stubSnapshots{
		resp: domaingames.TodayResponse{
			Date:  "2024-02-01",
			Games: []domaingames.Game{{ID: "snapshot-game"}},
		},
	}

	svc := testutil.NewServiceWithGames(nil)
	h := NewHandler(svc, snaps, nil, nil)
	h.now = func() time.Time { return time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC) }

	rr := testutil.Serve(h, http.MethodGet, "/games/today", nil)
	testutil.AssertStatus(t, rr, http.StatusOK)
	var resp domaingames.TodayResponse
	testutil.DecodeJSON(t, rr, &resp)
	if resp.Date != "2024-02-01" || len(resp.Games) != 1 || resp.Games[0].ID != "snapshot-game" {
		t.Fatalf("unexpected snapshot response %+v", resp)
	}
}

func TestWriteJSONErrorPath(t *testing.T) {
	rr := httptest.NewRecorder()
	// channels cannot be JSON encoded; triggers the error branch.
	writeJSON(rr, http.StatusOK, make(chan int), nil)
	// Status is still written even on encode error.
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 despite encode error, got %d", rr.Code)
	}
}

// Helper coverage
func TestWriteJSONErrorLogs(t *testing.T) {
	logger, buf := testutil.NewBufferLogger()
	rr := httptest.NewRecorder()
	writeJSON(rr, http.StatusOK, make(chan int), logger)
	if buf.Len() == 0 {
		t.Fatalf("expected logger to record encode error")
	}
}

func TestWriteErrorIncludesHeaderRequestID(t *testing.T) {
	logger, _ := testutil.NewBufferLogger()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/games/today", nil)
	req.Header.Set("X-Request-ID", "from-header")

	writeError(rr, req, http.StatusBadRequest, "boom", logger)

	var resp map[string]string
	testutil.DecodeJSON(t, rr, &resp)
	if resp["requestId"] != "from-header" {
		t.Fatalf("expected requestId from header, got %s", resp["requestId"])
	}
	if resp["error"] != "boom" {
		t.Fatalf("expected error message propagated")
	}
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
	req := httptest.NewRequest(http.MethodGet, "/games/today", nil).WithContext(ctx)

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
func BenchmarkGamesToday(b *testing.B) {
	ms := store.NewMemoryStore()
	now := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	ms.SetGames([]domaingames.Game{
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
	})
	svc := games.NewService(ms)
	h := NewHandler(svc, snapshots.NewFSStore(b.TempDir()), nil, nil)
	h.now = func() time.Time { return now }

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/games/today", nil)
		h.GamesToday(rr, req)
	}
}

func BenchmarkGameByID(b *testing.B) {
	ms := store.NewMemoryStore()
	now := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	ms.SetGames([]domaingames.Game{
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
	})
	svc := games.NewService(ms)
	h := NewHandler(svc, nil, nil, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/games/game-1", nil)
		h.GameByID(rr, req)
	}
}
