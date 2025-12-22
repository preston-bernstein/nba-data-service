package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"nba-games-service/internal/config"
	"nba-games-service/internal/domain"
	httpserver "nba-games-service/internal/http"
	"nba-games-service/internal/logging"
	"nba-games-service/internal/metrics"
	"nba-games-service/internal/poller"
	"nba-games-service/internal/providers"
	"nba-games-service/internal/store"
)

type Server struct {
	cfg           config.Config
	logger        *slog.Logger
	metrics       *metrics.Recorder
	domainService *domain.Service
	httpServer    httpServer
	metricsServer httpServer
	poller        Poller
	metricsStop   func(context.Context) error
}

// New constructs a server with default provider and poller wiring.
func New(cfg config.Config, logger *slog.Logger) *Server {
	provider := selectProvider(cfg, logger)
	return newServerWithProvider(cfg, logger, provider)
}

func newServerWithProvider(cfg config.Config, logger *slog.Logger, provider providers.GameProvider) *Server {
	return newServerWithMetrics(cfg, logger, provider, nil)
}

func newServerWithMetrics(cfg config.Config, logger *slog.Logger, provider providers.GameProvider, recorder *metrics.Recorder) *Server {
	var metricsShutdown func(context.Context) error
	var metricsSrv httpServer
	providerName := normalizeProviderName(cfg.Provider, provider)

	if recorder == nil {
		recCfg := metrics.TelemetryConfig{
			Enabled:      cfg.Metrics.Enabled,
			Port:         cfg.Metrics.Port,
			ServiceName:  cfg.Metrics.ServiceName,
			OtlpEndpoint: cfg.Metrics.OtlpEndpoint,
			OtlpInsecure: cfg.Metrics.OtlpInsecure,
		}
		rec, handler, shutdown, err := metrics.Setup(context.Background(), recCfg)
		if err != nil {
			if logger != nil {
				logger.Warn("metrics setup failed, continuing without telemetry", "err", err)
			}
			recorder = metrics.NewRecorder()
		} else {
			recorder = rec
			metricsShutdown = shutdown
			if handler != nil && recCfg.Enabled {
				metricsSrv = netHTTPServer{
					srv: &http.Server{
						Addr:    ":" + recCfg.Port,
						Handler: handler,
					},
				}
			}
		}
	}

	provider = providers.NewRetryingProvider(provider, logger, recorder, providerName, 0, 0)
	domainService := buildDomainService()
	plr := poller.New(provider, domainService, logger, recorder, cfg.PollInterval)
	httpSrv := buildHTTPServer(cfg, domainService, logger, provider, recorder, plr)

	return &Server{
		cfg:           cfg,
		logger:        logger,
		metrics:       recorder,
		domainService: domainService,
		httpServer:    httpSrv,
		metricsServer: metricsSrv,
		poller:        plr,
		metricsStop:   metricsShutdown,
	}
}

// newServerWithDeps is used for testing to inject custom components.
func newServerWithDeps(cfg config.Config, logger *slog.Logger, svc *domain.Service, httpSrv httpServer, plr Poller) *Server {
	return &Server{
		cfg:           cfg,
		logger:        logger,
		domainService: svc,
		httpServer:    httpSrv,
		poller:        plr,
	}
}

func buildDomainService() *domain.Service {
	memoryStore := store.NewMemoryStore()
	return domain.NewService(memoryStore)
}

func buildHTTPServer(cfg config.Config, svc *domain.Service, logger *slog.Logger, provider providers.GameProvider, recorder *metrics.Recorder, plr Poller) httpServer {
	var statusFn func() poller.Status
	if plr != nil {
		statusFn = plr.Status
	}

	handler := httpserver.NewHandler(svc, logger, provider, statusFn)
	router := httpserver.NewRouter(handler)
	if logger == nil {
		logger = logging.NewLogger(logging.Config{})
	}
	wrapped := httpserver.LoggingMiddleware(logger, recorder, router)

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

func normalizeProviderName(raw string, provider providers.GameProvider) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw != "" {
		return raw
	}
	if provider != nil {
		return strings.ToLower(fmt.Sprintf("%T", provider))
	}
	return "provider"
}

// Handler exposes the HTTP handler (useful for tests).
func (s *Server) Handler() http.Handler {
	return s.httpServer.Handler()
}
