package server

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/preston-bernstein/nba-data-service/internal/app/games"
	"github.com/preston-bernstein/nba-data-service/internal/app/players"
	"github.com/preston-bernstein/nba-data-service/internal/app/teams"
	"github.com/preston-bernstein/nba-data-service/internal/config"
	httpserver "github.com/preston-bernstein/nba-data-service/internal/http"
	"github.com/preston-bernstein/nba-data-service/internal/http/handlers"
	"github.com/preston-bernstein/nba-data-service/internal/http/middleware"
	"github.com/preston-bernstein/nba-data-service/internal/logging"
	"github.com/preston-bernstein/nba-data-service/internal/metrics"
	"github.com/preston-bernstein/nba-data-service/internal/poller"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
	"github.com/preston-bernstein/nba-data-service/internal/store"
)

var metricsSetup = metrics.Setup

type Server struct {
	cfg            config.Config
	logger         *slog.Logger
	metrics        *metrics.Recorder
	store          *store.MemoryStore
	gamesService   *games.Service
	teamsService   *teams.Service
	playersService *players.Service
	httpServer     httpServer
	metricsServer  httpServer
	poller         Poller
	metricsStop    func(context.Context) error
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
		provider = providers.NewRetryingProvider(provider, logger, recorder, normalizeProviderName(cfg.Provider, provider), 0, 0)
	}
	memoryStore, gameSvc, teamSvc, playerSvc := buildServices()
	plr := poller.New(provider, gameSvc, logger, recorder, cfg.PollInterval)
	httpSrv := buildHTTPServer(cfg, memoryStore, gameSvc, teamSvc, playerSvc, logger, provider, recorder, plr)

	return &Server{
		cfg:            cfg,
		logger:         logger,
		metrics:        recorder,
		store:          memoryStore,
		gamesService:   gameSvc,
		teamsService:   teamSvc,
		playersService: playerSvc,
		httpServer:     httpSrv,
		metricsServer:  metricsSrv,
		poller:         plr,
		metricsStop:    metricsShutdown,
	}
}

// newServerWithDeps is used for testing to inject custom components.
func newServerWithDeps(cfg config.Config, logger *slog.Logger, gameSvc *games.Service, httpSrv httpServer, plr Poller) *Server {
	return &Server{
		cfg:            cfg,
		logger:         logger,
		store:          nil,
		gamesService:   gameSvc,
		teamsService:   nil,
		playersService: nil,
		httpServer:     httpSrv,
		poller:         plr,
	}
}

func buildServices() (*store.MemoryStore, *games.Service, *teams.Service, *players.Service) {
	memoryStore := store.NewMemoryStore()
	return memoryStore, games.NewService(memoryStore), teams.NewService(memoryStore), players.NewService(memoryStore)
}

func buildHTTPServer(cfg config.Config, memoryStore *store.MemoryStore, gameSvc *games.Service, teamSvc *teams.Service, playerSvc *players.Service, logger *slog.Logger, provider providers.GameProvider, recorder *metrics.Recorder, plr Poller) httpServer {
	var statusFn func() poller.Status
	if plr != nil {
		statusFn = plr.Status
	}

	snaps := buildSnapshots(cfg, provider, memoryStore, logger)
	handler := handlers.NewHandler(gameSvc, teamSvc, playerSvc, snaps.store, logger, statusFn)
	admin := handlers.NewAdminHandler(gameSvc, snaps.writer, provider, cfg.Snapshots.AdminToken, logger)
	router := httpserver.NewRouter(handler)
	// Optionally mount admin refresh endpoint if token is set.
	if admin != nil && cfg.Snapshots.AdminToken != "" {
		if mux, ok := router.(*http.ServeMux); ok {
			mux.HandleFunc("/admin/snapshots/refresh", admin.RefreshSnapshots)
		}
	}
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

	s.gracefulShutdown()
}

func (s *Server) startServer(stop context.CancelFunc) {
	if s.logger != nil {
		s.logger.Info("http server starting", slog.String("addr", s.httpServer.Addr()))
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
		s.logger.Info("metrics server starting", slog.String("addr", s.metricsServer.Addr()))
	}
	launchServer("metrics", s.metricsServer, s.logger, nil)
}

func (s *Server) gracefulShutdown() {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if s.metricsStop != nil {
		if err := s.metricsStop(shutdownCtx); err != nil && s.logger != nil {
			s.logger.Warn("metrics shutdown failed", "error", err)
		}
	}

	if s.metricsServer != nil {
		if err := s.metricsServer.Shutdown(shutdownCtx); err != nil && s.logger != nil {
			s.logger.Warn("metrics server shutdown failed", "error", err)
		}
	}

	if err := s.poller.Stop(shutdownCtx); err != nil && s.logger != nil {
		s.logger.Error("failed to stop poller", "error", err)
	}

	if err := s.httpServer.Shutdown(shutdownCtx); err != nil && s.logger != nil {
		s.logger.Error("graceful shutdown failed", "error", err)
	}

	// Stop rate-limited providers to avoid ticker leaks when present.
	if rl, ok := s.pollerProvider().(interface{ Close() }); ok {
		rl.Close()
	}

	if s.logger != nil {
		s.logger.Info("shutdown complete")
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
			srv: &http.Server{
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
			logger.Info("starting "+name+" server", slog.String("addr", srv.Addr()))
		}
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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
