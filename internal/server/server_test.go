package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"nba-data-service/internal/config"
	"nba-data-service/internal/domain"
	"nba-data-service/internal/metrics"
	"nba-data-service/internal/poller"
	"nba-data-service/internal/providers"
	"nba-data-service/internal/providers/balldontlie"
	"nba-data-service/internal/testutil"
)

func TestServerServesHealthAndGames(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	game := testutil.SampleGame("stub-1")
	game.StartTime = time.Date(2024, 1, 1, 15, 0, 0, 0, time.UTC).Format(time.RFC3339)

	provider := &testutil.NotifyingProvider{
		Games:  []domain.Game{game},
		Notify: make(chan struct{}),
	}

	cfg := config.Config{PollInterval: 5 * time.Millisecond}
	srv := newServerWithProvider(cfg, nil, provider)
	srv.poller.Start(ctx)

	select {
	case <-provider.Notify:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for poller to fetch")
	}

	router := srv.Handler()

	healthRec := testutil.Serve(router, http.MethodGet, "/health", nil)
	testutil.AssertStatus(t, healthRec, http.StatusOK)

	gamesRec := testutil.Serve(router, http.MethodGet, "/games/today", nil)
	testutil.AssertStatus(t, gamesRec, http.StatusOK)

	var today domain.TodayResponse
	testutil.DecodeJSON(t, gamesRec, &today)

	if len(today.Games) != 1 {
		t.Fatalf("expected 1 game, got %d", len(today.Games))
	}
	if today.Games[0].ID != "stub-1" {
		t.Fatalf("unexpected game id %s", today.Games[0].ID)
	}
}

func TestProviderFactoryWrapsProvider(t *testing.T) {
	cfg := config.Config{Provider: "fixture"}
	factory := newProviderFactory(nil, metrics.NewRecorder())
	provider := factory.build(cfg)
	if provider == nil {
		t.Fatalf("expected provider")
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
	stub := &testutil.StubHTTPServer{AddrVal: "addr", ListenErr: http.ErrServerClosed}
	s := &Server{
		metricsServer: stub,
	}
	s.startMetrics()
	time.Sleep(10 * time.Millisecond)
	if stub.ListenCalls == 0 {
		t.Fatalf("expected metrics server to start")
	}
}

func TestGracefulShutdownStopsAll(t *testing.T) {
	stubSrv := &testutil.StubHTTPServer{}
	stubMetrics := &testutil.StubHTTPServer{}
	stubPoller := &testutil.StubPoller{}
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
	if stubMetrics.ShutdownCalls != 1 {
		t.Fatalf("expected metrics server shutdown, got %d", stubMetrics.ShutdownCalls)
	}
	if stubPoller.StopCalls != 1 {
		t.Fatalf("expected poller stop, got %d", stubPoller.StopCalls)
	}
	if stubSrv.ShutdownCalls != 1 {
		t.Fatalf("expected http server shutdown, got %d", stubSrv.ShutdownCalls)
	}
}

func TestGracefulShutdownLogsErrors(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	stubSrv := &testutil.StubHTTPServer{ShutdownErr: errors.New("srv err")}
	stubMetrics := &testutil.StubHTTPServer{ShutdownErr: errors.New("metrics err")}
	stubPoller := &testutil.StubPoller{Err: errors.New("poller err")}

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
	srv := newServerWithProvider(cfg, nil, &testutil.ErrProvider{Err: context.DeadlineExceeded})
	srv.poller.Start(ctx)

	// Give the poller a moment to attempt a fetch.
	time.Sleep(20 * time.Millisecond)

	router := srv.Handler()
	gamesRec := testutil.Serve(router, http.MethodGet, "/games/today", nil)

	testutil.AssertStatus(t, gamesRec, http.StatusOK)

	var today domain.TodayResponse
	testutil.DecodeJSON(t, gamesRec, &today)

	if len(today.Games) != 0 {
		t.Fatalf("expected no games when provider errors, got %d", len(today.Games))
	}
}

func TestGracefulShutdownCallsStopAndShutdown(t *testing.T) {
	svc := testutil.NewServiceWithGames(nil)
	p := &testutil.StubPoller{}
	httpSrv := &testutil.StubHTTPServer{}

	srv := newServerWithDeps(config.Config{}, nil, svc, httpSrv, p)
	srv.gracefulShutdown()

	if p.StopCalls != 1 {
		t.Fatalf("expected poller Stop to be called once, got %d", p.StopCalls)
	}
	if httpSrv.ShutdownCalls != 1 {
		t.Fatalf("expected server Shutdown to be called once, got %d", httpSrv.ShutdownCalls)
	}
}

func TestGracefulShutdownTimesOutLongRunningShutdown(t *testing.T) {
	svc := testutil.NewServiceWithGames(nil)
	p := &testutil.StubPoller{}

	blocking := &testutil.BlockingHTTPServer{
		AddrVal:    ":0",
		HandlerVal: http.NewServeMux(),
		Unblock:    make(chan struct{}),
	}

	original := shutdownTimeout
	shutdownTimeout = 5 * time.Millisecond
	defer func() { shutdownTimeout = original }()

	srv := newServerWithDeps(config.Config{}, nil, svc, blocking, p)

	start := time.Now()
	srv.gracefulShutdown()
	elapsed := time.Since(start)

	if blocking.ShutdownCalls != 1 {
		t.Fatalf("expected server Shutdown to be called once, got %d", blocking.ShutdownCalls)
	}
	if p.StopCalls != 1 {
		t.Fatalf("expected poller Stop to be called once, got %d", p.StopCalls)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("shutdown took too long: %s", elapsed)
	}
}

func TestGracefulShutdownContinuesWhenPollerStopErrors(t *testing.T) {
	svc := testutil.NewServiceWithGames(nil)
	p := &testutil.StubPoller{Err: errors.New("stop failure")}
	httpSrv := &testutil.StubHTTPServer{}

	srv := newServerWithDeps(config.Config{}, nil, svc, httpSrv, p)
	srv.gracefulShutdown()

	if p.StopCalls != 1 {
		t.Fatalf("expected poller Stop to be called once, got %d", p.StopCalls)
	}
	if httpSrv.ShutdownCalls != 1 {
		t.Fatalf("expected server Shutdown to be called once, got %d", httpSrv.ShutdownCalls)
	}
}

type closableProvider struct{ closed bool }

func (c *closableProvider) FetchGames(ctx context.Context, date, tz string) ([]domain.Game, error) {
	return nil, nil
}

func (c *closableProvider) Close() {
	c.closed = true
}

type providerPoller struct {
	provider providers.GameProvider
	stop     int
}

func (p *providerPoller) Start(ctx context.Context)      {}
func (p *providerPoller) Stop(ctx context.Context) error { p.stop++; return nil }
func (p *providerPoller) Status() poller.Status          { return poller.Status{} }
func (p *providerPoller) Provider() providers.GameProvider {
	return p.provider
}

func TestGracefulShutdownClosesRateLimitedProvider(t *testing.T) {
	svc := testutil.NewServiceWithGames(nil)
	httpSrv := &testutil.StubHTTPServer{}
	prov := &closableProvider{}
	pp := &providerPoller{provider: prov}

	srv := newServerWithDeps(config.Config{}, nil, svc, httpSrv, pp)

	srv.gracefulShutdown()

	if !prov.closed {
		t.Fatalf("expected provider Close to be called")
	}
	if pp.stop != 1 {
		t.Fatalf("expected poller Stop to be called once, got %d", pp.stop)
	}
}

func TestPollerProviderReturnsNilWhenUnavailable(t *testing.T) {
	svc := testutil.NewServiceWithGames(nil)
	plr := &testutil.StubPoller{}
	httpSrv := &testutil.StubHTTPServer{}

	srv := newServerWithDeps(config.Config{}, nil, svc, httpSrv, plr)

	if got := srv.pollerProvider(); got != nil {
		t.Fatalf("expected nil provider when poller does not expose it")
	}
}

func TestServerHandlerSetsRequestIDHeader(t *testing.T) {
	cfg := config.Config{
		Port:     "0",
		Provider: "fixture",
		Snapshots: config.SnapshotSyncConfig{
			SnapshotFolder: t.TempDir(),
		},
	}
	srv := New(cfg, nil)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	srv.Handler().ServeHTTP(rr, req)
	if got := rr.Header().Get("X-Request-ID"); got == "" {
		t.Fatalf("expected X-Request-ID header set by middleware")
	}
}

func TestAdminRouteMountedOnlyWithToken(t *testing.T) {
	cfg := config.Config{
		Port: "0",
		Snapshots: config.SnapshotSyncConfig{
			Enabled:        true,
			SnapshotFolder: t.TempDir(),
			AdminToken:     "secret",
		},
		Provider: "fixture",
	}
	srv := New(cfg, nil)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/admin/snapshots/refresh", nil)
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected admin route mounted with 401 without token, got %d", rr.Code)
	}

	cfg.Snapshots.AdminToken = ""
	srv = New(cfg, nil)
	rr = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected admin route absent without token, got %d", rr.Code)
	}
}

func TestBuildMetricsUsesFallbackOnSetupError(t *testing.T) {
	cfg := config.Config{
		Metrics: config.MetricsConfig{Enabled: true},
	}
	orig := metricsSetup
	defer func() { metricsSetup = orig }()
	metricsSetup = func(ctx context.Context, cfg metrics.TelemetryConfig) (*metrics.Recorder, http.Handler, func(context.Context) error, error) {
		return nil, nil, nil, errors.New("boom")
	}

	rec, srv, stop := buildMetrics(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if rec == nil {
		t.Fatalf("expected recorder even on setup error")
	}
	if srv != nil || stop != nil {
		t.Fatalf("expected metrics server/shutdown nil when setup fails")
	}
}

func TestServerStartHandlesListenErrorAndStops(t *testing.T) {
	svc := testutil.NewServiceWithGames(nil)
	plr := &testutil.StubPoller{}
	httpSrv := &testutil.ErrHTTPServer{}

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

func TestRunCancelsAndStopsComponents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svc := testutil.NewServiceWithGames(nil)
	plr := &testutil.StubPoller{}
	httpSrv := &testutil.CloseableHTTPServer{}

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

	if plr.StartCalls != 1 {
		t.Fatalf("expected poller Start called once, got %d", plr.StartCalls)
	}
	if plr.StopCalls != 1 {
		t.Fatalf("expected poller Stop called once, got %d", plr.StopCalls)
	}
	if httpSrv.ShutdownCalls != 1 {
		t.Fatalf("expected server Shutdown called once, got %d", httpSrv.ShutdownCalls)
	}
}
