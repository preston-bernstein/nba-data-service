package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"nba-games-service/internal/config"
	"nba-games-service/internal/domain"
	"nba-games-service/internal/poller"
	"nba-games-service/internal/providers/balldontlie"
	"nba-games-service/internal/store"
)

type stubProvider struct {
	games  []domain.Game
	notify chan struct{}
}

func (s *stubProvider) FetchGames(ctx context.Context, date string, tz string) ([]domain.Game, error) {
	_ = ctx
	_ = date
	_ = tz
	if s.notify != nil {
		select {
		case <-s.notify:
		default:
			close(s.notify)
		}
	}
	return s.games, nil
}

type errProvider struct{}

func (e *errProvider) FetchGames(ctx context.Context, date string, tz string) ([]domain.Game, error) {
	_ = ctx
	_ = date
	_ = tz
	return nil, context.DeadlineExceeded
}

type stubPoller struct {
	startCalls int
	stopCalls  int
	err        error
	status     poller.Status
}

func (p *stubPoller) Start(ctx context.Context) {
	_ = ctx
	p.startCalls++
}

func (p *stubPoller) Stop(ctx context.Context) error {
	_ = ctx
	p.stopCalls++
	return p.err
}

func (p *stubPoller) Status() poller.Status {
	return p.status
}

type stubHTTPServer struct {
	addr          string
	handler       http.Handler
	listenCalls   int
	shutdownCalls int
	listenErr     error
	shutdownErr   error
}

func (s *stubHTTPServer) ListenAndServe() error {
	s.listenCalls++
	return s.listenErr
}

func (s *stubHTTPServer) Shutdown(ctx context.Context) error {
	_ = ctx
	s.shutdownCalls++
	return s.shutdownErr
}

func (s *stubHTTPServer) Addr() string {
	return s.addr
}

func (s *stubHTTPServer) Handler() http.Handler {
	return s.handler
}

type blockingHTTPServer struct {
	addr          string
	handler       http.Handler
	shutdownCalls int
	unblock       chan struct{}
}

func (s *blockingHTTPServer) ListenAndServe() error {
	return nil
}

func (s *blockingHTTPServer) Shutdown(ctx context.Context) error {
	s.shutdownCalls++
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.unblock:
		return nil
	}
}

func (s *blockingHTTPServer) Addr() string {
	return s.addr
}

func (s *blockingHTTPServer) Handler() http.Handler {
	return s.handler
}

func TestServerServesHealthAndGames(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	game := domain.Game{
		ID:        "stub-1",
		Provider:  "stub",
		HomeTeam:  domain.Team{ID: "home", Name: "Home", ExternalID: 1},
		AwayTeam:  domain.Team{ID: "away", Name: "Away", ExternalID: 2},
		StartTime: time.Date(2024, 1, 1, 15, 0, 0, 0, time.UTC).Format(time.RFC3339),
		Status:    domain.StatusScheduled,
		Score:     domain.Score{Home: 0, Away: 0},
		Meta:      domain.GameMeta{Season: "2023-2024", UpstreamGameID: 10},
	}

	provider := &stubProvider{
		games:  []domain.Game{game},
		notify: make(chan struct{}),
	}

	cfg := config.Config{PollInterval: 5 * time.Millisecond}
	srv := newServerWithProvider(cfg, nil, provider)
	srv.poller.Start(ctx)

	select {
	case <-provider.notify:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for poller to fetch")
	}

	router := srv.Handler()

	healthReq := httptest.NewRequest(http.MethodGet, "/health", nil)
	healthRec := httptest.NewRecorder()
	router.ServeHTTP(healthRec, healthReq)

	if healthRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /health, got %d", healthRec.Code)
	}

	gamesReq := httptest.NewRequest(http.MethodGet, "/games/today", nil)
	gamesRec := httptest.NewRecorder()
	router.ServeHTTP(gamesRec, gamesReq)

	if gamesRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /games/today, got %d", gamesRec.Code)
	}

	var today domain.TodayResponse
	if err := json.NewDecoder(gamesRec.Body).Decode(&today); err != nil {
		t.Fatalf("failed to decode games response: %v", err)
	}

	if len(today.Games) != 1 {
		t.Fatalf("expected 1 game, got %d", len(today.Games))
	}
	if today.Games[0].ID != "stub-1" {
		t.Fatalf("unexpected game id %s", today.Games[0].ID)
	}
}

func TestSelectProviderFallsBackToFixture(t *testing.T) {
	provider := selectProvider(config.Config{Provider: "unknown"}, nil)
	if provider == nil {
		t.Fatalf("expected provider fallback")
	}
}

func TestSelectProviderChoosesBalldontlie(t *testing.T) {
	provider := selectProvider(config.Config{
		Provider: "balldontlie",
		Balldontlie: config.BalldontlieConfig{
			BaseURL: "http://example.com",
			APIKey:  "key",
		},
	}, nil)
	if _, ok := provider.(*balldontlie.Client); !ok {
		t.Fatalf("expected balldontlie provider")
	}
}

func TestSelectProviderDefaultsToFixture(t *testing.T) {
	provider := selectProvider(config.Config{}, nil)
	if provider == nil {
		t.Fatalf("expected provider")
	}
}

func TestSelectProviderFixtureExplicit(t *testing.T) {
	provider := selectProvider(config.Config{Provider: "fixture"}, nil)
	if provider == nil {
		t.Fatalf("expected fixture provider")
	}
}

func TestNewConstructsServer(t *testing.T) {
	cfg := config.Config{
		Port:     "0",
		Provider: "fixture",
		Metrics: config.MetricsConfig{
			Enabled: false,
		},
	}
	srv := New(cfg, nil)
	if srv == nil || srv.Handler() == nil {
		t.Fatalf("expected server with handler")
	}
}

func TestNormalizeProviderName(t *testing.T) {
	if got := normalizeProviderName("Balldontlie", nil); got != "balldontlie" {
		t.Fatalf("expected lowercase raw, got %s", got)
	}
	provider := selectProvider(config.Config{Provider: "fixture"}, nil)
	if got := normalizeProviderName("", provider); got == "" || got == "provider" {
		t.Fatalf("expected derived provider name, got %s", got)
	}
	if got := normalizeProviderName("", nil); got != "provider" {
		t.Fatalf("expected fallback provider, got %s", got)
	}
}
func TestStartMetricsSkipsWhenNil(t *testing.T) {
	s := &Server{}
	s.startMetrics() // should no-op without panic
}

func TestStartMetricsUsesServer(t *testing.T) {
	stub := &stubHTTPServer{addr: "addr", listenErr: http.ErrServerClosed}
	s := &Server{
		metricsServer: stub,
	}
	s.startMetrics()
	time.Sleep(10 * time.Millisecond)
	if stub.listenCalls == 0 {
		t.Fatalf("expected metrics server to start")
	}
}

func TestGracefulShutdownStopsAll(t *testing.T) {
	stubSrv := &stubHTTPServer{}
	stubMetrics := &stubHTTPServer{}
	stubPoller := &stubPoller{}
	metricsStopped := 0

	s := &Server{
		httpServer:    stubSrv,
		metricsServer: stubMetrics,
		poller:        stubPoller,
		metricsStop: func(ctx context.Context) error {
			_ = ctx
			metricsStopped++
			return nil
		},
	}

	s.gracefulShutdown()

	if metricsStopped != 1 {
		t.Fatalf("expected metricsStop called, got %d", metricsStopped)
	}
	if stubMetrics.shutdownCalls != 1 {
		t.Fatalf("expected metrics server shutdown, got %d", stubMetrics.shutdownCalls)
	}
	if stubPoller.stopCalls != 1 {
		t.Fatalf("expected poller stop, got %d", stubPoller.stopCalls)
	}
	if stubSrv.shutdownCalls != 1 {
		t.Fatalf("expected http server shutdown, got %d", stubSrv.shutdownCalls)
	}
}

func TestGracefulShutdownLogsErrors(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	stubSrv := &stubHTTPServer{shutdownErr: errors.New("srv err")}
	stubMetrics := &stubHTTPServer{shutdownErr: errors.New("metrics err")}
	stubPoller := &stubPoller{err: errors.New("poller err")}

	s := &Server{
		logger:        logger,
		httpServer:    stubSrv,
		metricsServer: stubMetrics,
		poller:        stubPoller,
		metricsStop: func(ctx context.Context) error {
			_ = ctx
			return errors.New("metrics stop err")
		},
	}

	s.gracefulShutdown()
}

func TestServerHandlesProviderErrorGracefully(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Config{PollInterval: 5 * time.Millisecond}
	srv := newServerWithProvider(cfg, nil, &errProvider{})
	srv.poller.Start(ctx)

	// Give the poller a moment to attempt a fetch.
	time.Sleep(20 * time.Millisecond)

	router := srv.Handler()
	gamesReq := httptest.NewRequest(http.MethodGet, "/games/today", nil)
	gamesRec := httptest.NewRecorder()
	router.ServeHTTP(gamesRec, gamesReq)

	if gamesRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /games/today, got %d", gamesRec.Code)
	}

	var today domain.TodayResponse
	if err := json.NewDecoder(gamesRec.Body).Decode(&today); err != nil {
		t.Fatalf("failed to decode games response: %v", err)
	}

	if len(today.Games) != 0 {
		t.Fatalf("expected no games when provider errors, got %d", len(today.Games))
	}
}

func TestGracefulShutdownCallsStopAndShutdown(t *testing.T) {
	svc := domain.NewService(store.NewMemoryStore())
	p := &stubPoller{}
	httpSrv := &stubHTTPServer{}

	srv := newServerWithDeps(config.Config{}, nil, svc, httpSrv, p)
	srv.gracefulShutdown()

	if p.stopCalls != 1 {
		t.Fatalf("expected poller Stop to be called once, got %d", p.stopCalls)
	}
	if httpSrv.shutdownCalls != 1 {
		t.Fatalf("expected server Shutdown to be called once, got %d", httpSrv.shutdownCalls)
	}
}

func TestGracefulShutdownTimesOutLongRunningShutdown(t *testing.T) {
	svc := domain.NewService(store.NewMemoryStore())
	p := &stubPoller{}

	blocking := &blockingHTTPServer{
		addr:    ":0",
		handler: http.NewServeMux(),
		unblock: make(chan struct{}),
	}

	original := shutdownTimeout
	shutdownTimeout = 5 * time.Millisecond
	defer func() { shutdownTimeout = original }()

	srv := newServerWithDeps(config.Config{}, nil, svc, blocking, p)

	start := time.Now()
	srv.gracefulShutdown()
	elapsed := time.Since(start)

	if blocking.shutdownCalls != 1 {
		t.Fatalf("expected server Shutdown to be called once, got %d", blocking.shutdownCalls)
	}
	if p.stopCalls != 1 {
		t.Fatalf("expected poller Stop to be called once, got %d", p.stopCalls)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("shutdown took too long: %s", elapsed)
	}
}

func TestGracefulShutdownContinuesWhenPollerStopErrors(t *testing.T) {
	svc := domain.NewService(store.NewMemoryStore())
	p := &stubPoller{err: errors.New("stop failure")}
	httpSrv := &stubHTTPServer{}

	srv := newServerWithDeps(config.Config{}, nil, svc, httpSrv, p)
	srv.gracefulShutdown()

	if p.stopCalls != 1 {
		t.Fatalf("expected poller Stop to be called once, got %d", p.stopCalls)
	}
	if httpSrv.shutdownCalls != 1 {
		t.Fatalf("expected server Shutdown to be called once, got %d", httpSrv.shutdownCalls)
	}
}

type errHTTPServer struct {
	shutdownCalls int
}

func (e *errHTTPServer) ListenAndServe() error {
	return errors.New("listen failure")
}

func (e *errHTTPServer) Shutdown(ctx context.Context) error {
	_ = ctx
	e.shutdownCalls++
	return nil
}

func (e *errHTTPServer) Addr() string {
	return ":0"
}

func (e *errHTTPServer) Handler() http.Handler {
	return http.NewServeMux()
}

func TestServerStartHandlesListenErrorAndStops(t *testing.T) {
	svc := domain.NewService(store.NewMemoryStore())
	plr := &stubPoller{}
	httpSrv := &errHTTPServer{}

	srv := newServerWithDeps(config.Config{}, nil, svc, httpSrv, plr)

	var wg sync.WaitGroup
	wg.Add(1)
	stopCalled := make(chan struct{})
	stop := func() {
		close(stopCalled)
		wg.Done()
	}

	srv.startServer(stop)

	select {
	case <-stopCalled:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected stop to be called on listen failure")
	}

	wg.Wait()
}

type closeableHTTPServer struct {
	shutdownCalls int
}

func (c *closeableHTTPServer) ListenAndServe() error {
	return http.ErrServerClosed
}

func (c *closeableHTTPServer) Shutdown(ctx context.Context) error {
	_ = ctx
	c.shutdownCalls++
	return nil
}

func (c *closeableHTTPServer) Addr() string {
	return ":0"
}

func (c *closeableHTTPServer) Handler() http.Handler {
	return http.NewServeMux()
}

func TestRunCancelsAndStopsComponents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svc := domain.NewService(store.NewMemoryStore())
	plr := &stubPoller{}
	httpSrv := &closeableHTTPServer{}

	srv := newServerWithDeps(config.Config{}, nil, svc, httpSrv, plr)

	done := make(chan struct{})
	go func() {
		srv.Run(ctx, cancel)
		close(done)
	}()

	// Let Start be invoked.
	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("run did not return after cancel")
	}

	if plr.startCalls != 1 {
		t.Fatalf("expected poller Start called once, got %d", plr.startCalls)
	}
	if plr.stopCalls != 1 {
		t.Fatalf("expected poller Stop called once, got %d", plr.stopCalls)
	}
	if httpSrv.shutdownCalls != 1 {
		t.Fatalf("expected server Shutdown called once, got %d", httpSrv.shutdownCalls)
	}
}
