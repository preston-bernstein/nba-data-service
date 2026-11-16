package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/preston-bernstein/nba-data-service/internal/config"
	"github.com/preston-bernstein/nba-data-service/internal/events"
	"github.com/preston-bernstein/nba-data-service/internal/gamedate"
	httpserver "github.com/preston-bernstein/nba-data-service/internal/http"
	"github.com/preston-bernstein/nba-data-service/internal/http/handlers"
	"github.com/preston-bernstein/nba-data-service/internal/http/middleware"
	"github.com/preston-bernstein/nba-data-service/internal/logging"
	"github.com/preston-bernstein/nba-data-service/internal/metrics"
	"github.com/preston-bernstein/nba-data-service/internal/poller"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
)

var metricsSetup = metrics.Setup

type Server struct {
	cfg           config.Config
	logger        *slog.Logger
	metrics       *metrics.Recorder
	httpServer    httpServer
	metricsServer httpServer
	poller        Poller
	metricsStop   func(context.Context) error
	publisher     events.Publisher
}

// New constructs a server with default provider and poller wiring.
func New(cfg config.Config, logger *slog.Logger) *Server {
	factory := newProviderFactory(logger, nil)
	provider := factory.build(cfg)
	return newServerWithProvider(cfg, logger, provider)
}

func newServerWithProvider(cfg config.Config, logger *slog.Logger, provider providers.GameProvider) *Server {
	return newServerWithMetrics(cfg, logger, provider, nil)
}

func newServerWithMetrics(cfg config.Config, logger *slog.Logger, provider providers.GameProvider, recorder *metrics.Recorder) *Server {
	recorder, metricsSrv, metricsShutdown := buildMetrics(cfg, logger, recorder)

	if provider == nil {
		provider = newProviderFactory(logger, recorder).build(cfg)
	} else {
		provider = providers.NewRetryingProvider(provider, providers.RetryConfig{Logger: logger, Metrics: recorder, Name: normalizeProviderName(cfg.Provider, provider)})
	}
	loc := gamedate.ResolveLocation(cfg.Balldontlie.Timezone)
	snaps := buildSnapshots(cfg, provider, logger, loc)
	plr := poller.New(provider, snaps.writer, poller.Options{Logger: logger, Metrics: recorder, Interval: cfg.PollInterval, Location: loc})
	pub := buildPublisher(cfg, logger)
	plr.SetPublisher(pub)
	var statusFn func() poller.Status
	if plr != nil {
		statusFn = plr.Status
	}
	router := buildRouter(routerDeps{
		snaps:      snaps,
		logger:     logger,
		status:     statusFn,
		provider:   provider,
		loc:        loc,
		adminToken: cfg.Snapshots.AdminToken,
	})
	httpSrv := buildHTTPServer(cfg, logger, recorder, router)

	return &Server{
		cfg:           cfg,
		logger:        logger,
		metrics:       recorder,
		httpServer:    httpSrv,
		metricsServer: metricsSrv,
		poller:        plr,
		metricsStop:   metricsShutdown,
		publisher:     pub,
	}
}

// newServerWithDeps is used for testing to inject custom components.
func newServerWithDeps(cfg config.Config, logger *slog.Logger, httpSrv httpServer, plr Poller) *Server {
	return &Server{
		cfg:        cfg,
		logger:     logger,
		httpServer: httpSrv,
		poller:     plr,
	}
}

// routerDeps holds what the HTTP routes need.
type routerDeps struct {
	snaps      snapshotComponents
	logger     *slog.Logger
	status     func() poller.Status
	provider   providers.GameProvider
	loc        *time.Location
	adminToken string
}

func buildRouter(deps routerDeps) http.Handler {
	handler := handlers.NewHandler(deps.snaps.store, deps.logger, deps.status, deps.loc)
	admin := handlers.NewAdminHandler(deps.snaps.writer, deps.provider, deps.adminToken, deps.logger)
	router := httpserver.NewRouter(handler)
	// Optionally mount admin refresh endpoint if token is set.
	if admin != nil && deps.adminToken != "" {
		if mux, ok := router.(*http.ServeMux); ok {
			mux.HandleFunc("/admin/snapshots/refresh", admin.RefreshSnapshots)
		}
	}
	return router
}

func buildHTTPServer(cfg config.Config, logger *slog.Logger, recorder *metrics.Recorder, router http.Handler) httpServer {
	if logger == nil {
		logger = logging.NewLogger(logging.Config{})
	}
	wrapped := middleware.LoggingMiddleware(logger, recorder, router)

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      wrapped,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}

	return netHTTPServer{srv: srv}
}

// Run starts the poller and HTTP server, then waits for context cancellation to shut down gracefully.
func (s *Server) Run(ctx context.Context, stop context.CancelFunc) {
	s.startMetrics()
	s.startServer(stop)
	s.poller.Start(ctx)

	<-ctx.Done()
	if s.logger != nil {
		s.logger.Info("shutdown signal received")
	}

	// The context is already cancelled here, so shutdown runs on a copy that keeps its values but not its cancellation.
	s.gracefulShutdown(context.WithoutCancel(ctx))
}

func (s *Server) startServer(stop context.CancelFunc) {
	if s.logger != nil {
		s.logger.Info("http server starting", "addr", s.httpServer.Addr())
	}
	launchServer("http", s.httpServer, s.logger, func(err error) {
		if stop != nil {
			stop()
		}
	})
}

func (s *Server) startMetrics() {
	if s.metricsServer == nil {
		return
	}
	if s.logger != nil {
		s.logger.Info("metrics server starting", "addr", s.metricsServer.Addr())
	}
	launchServer("metrics", s.metricsServer, s.logger, nil)
}

func (s *Server) gracefulShutdown(ctx context.Context) {
	shutdownCtx, cancel := context.WithTimeout(ctx, shutdownTimeout)
	defer cancel()

	if s.metricsStop != nil {
		s.logWarn("metrics shutdown failed", s.metricsStop(shutdownCtx))
	}
	if s.metricsServer != nil {
		s.logWarn("metrics server shutdown failed", s.metricsServer.Shutdown(shutdownCtx))
	}

	s.logError("failed to stop poller", s.poller.Stop(shutdownCtx))

	// Close after the poller stops, so no publish is in flight. Close flushes buffered records.
	if s.publisher != nil {
		s.publisher.Close()
	}

	s.logError("graceful shutdown failed", s.httpServer.Shutdown(shutdownCtx))

	// Stop rate-limited providers to avoid ticker leaks when present.
	if rl, ok := s.pollerProvider().(interface{ Close() }); ok {
		rl.Close()
	}

	if s.logger != nil {
		s.logger.Info("shutdown complete")
	}
}

func (s *Server) logWarn(msg string, err error) {
	if err != nil && s.logger != nil {
		s.logger.Warn(msg, "error", err)
	}
}

func (s *Server) logError(msg string, err error) {
	if err != nil && s.logger != nil {
		s.logger.Error(msg, "error", err)
	}
}

// pollerProvider attempts to extract the underlying provider from the poller when available.
// Best-effort helper to enable cleanup of rate-limited tickers; safe if not supported.
func (s *Server) pollerProvider() providers.GameProvider {
	if pa, ok := s.poller.(interface {
		Provider() providers.GameProvider
	}); ok {
		return pa.Provider()
	}
	return nil
}

func buildMetrics(cfg config.Config, logger *slog.Logger, recorder *metrics.Recorder) (*metrics.Recorder, httpServer, func(context.Context) error) {
	if recorder != nil {
		return recorder, nil, nil
	}

	recCfg := metrics.TelemetryConfig{
		Enabled:      cfg.Metrics.Enabled,
		Port:         cfg.Metrics.Port,
		ServiceName:  cfg.Metrics.ServiceName,
		OtlpEndpoint: cfg.Metrics.OtlpEndpoint,
		OtlpInsecure: cfg.Metrics.OtlpInsecure,
	}

	rec, handler, shutdown, err := metricsSetup(context.Background(), recCfg)
	if err != nil {
		if logger != nil {
			logger.Warn("metrics setup failed, continuing without telemetry", "err", err)
		}
		return metrics.NewRecorder(), nil, nil
	}

	var metricsSrv httpServer
	if handler != nil && recCfg.Enabled {
		metricsSrv = netHTTPServer{
			srv: &http.Server{ //nolint:gosec // metrics server has no timeouts today; adding one changes how slow clients are served
				Addr:    ":" + recCfg.Port,
				Handler: handler,
			},
		}
	}

	return rec, metricsSrv, shutdown
}

func launchServer(name string, srv httpServer, logger *slog.Logger, onError func(error)) {
	go func() {
		if logger != nil {
			logger.Info("starting "+name+" server", "addr", srv.Addr())
		}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			if logger != nil {
				logger.Warn(name+" server failed", "error", err)
			}
			if onError != nil {
				onError(err)
			}
		}
	}()
}

// Handler exposes the HTTP handler (useful for tests).
func (s *Server) Handler() http.Handler {
	return s.httpServer.Handler()
}
