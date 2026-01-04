package handlers

import (
	"context"
	"errors"
	"log/slog"
	nethttp "net/http"
	"net/url"
	"strings"
	"time"

	"github.com/preston-bernstein/nba-data-service/internal/app/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain"
	"github.com/preston-bernstein/nba-data-service/internal/poller"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
	"github.com/preston-bernstein/nba-data-service/internal/snapshots"
)

type nowFunc func() time.Time

// Handler wires HTTP routes to the domain service.
type Handler struct {
	svc      *games.Service
	snaps    snapshots.Store
	logger   *slog.Logger
	now      nowFunc
	statusFn func() poller.Status
}

// NewHandler constructs a Handler with defaults.
func NewHandler(svc *games.Service, snaps snapshots.Store, logger *slog.Logger, statusFn func() poller.Status) *Handler {
	return &Handler{
		svc:      svc,
		snaps:    snaps,
		logger:   logger,
		now:      time.Now,
		statusFn: statusFn,
	}
}

// Health reports the service health.
func (h *Handler) ServeHTTP(w nethttp.ResponseWriter, r *nethttp.Request) {
	switch {
	case r.URL.Path == "/health":
		h.Health(w, r)
	case r.URL.Path == "/ready":
		h.Ready(w, r)
	case r.URL.Path == "/games" || r.URL.Path == "/games/today":
		h.GamesToday(w, r)
	case strings.HasPrefix(r.URL.Path, "/games/"):
		h.GameByID(w, r)
	default:
		writeError(w, r, nethttp.StatusNotFound, "not found", h.logger)
	}
}

func (h *Handler) Health(w nethttp.ResponseWriter, r *nethttp.Request) {
	if r.Method != nethttp.MethodGet {
		writeError(w, r, nethttp.StatusMethodNotAllowed, "method not allowed", h.logger)
		return
	}
	if err := r.Context().Err(); err != nil {
		writeError(w, r, nethttp.StatusServiceUnavailable, "shutting down", h.logger)
		return
	}
	resp := map[string]string{"status": "ok"}
	writeJSON(w, nethttp.StatusOK, resp, h.logger)
}

// Ready reports readiness for traffic (e.g., for Kubernetes probes).
func (h *Handler) Ready(w nethttp.ResponseWriter, r *nethttp.Request) {
	if r.Method != nethttp.MethodGet {
		writeError(w, r, nethttp.StatusMethodNotAllowed, "method not allowed", h.logger)
		return
	}
	if h.statusFn == nil {
		writeJSON(w, nethttp.StatusOK, map[string]string{"status": "ready"}, h.logger)
		return
	}
	if h.statusFn().IsReady() {
		writeJSON(w, nethttp.StatusOK, map[string]string{"status": "ready"}, h.logger)
		return
	}
	msg := h.statusFn().LastError
	if msg == "" {
		msg = "not ready"
	}
	writeError(w, r, nethttp.StatusServiceUnavailable, msg, h.logger)
}

// GamesToday returns the current snapshot of games.
func (h *Handler) GamesToday(w nethttp.ResponseWriter, r *nethttp.Request) {
	if r.Method != nethttp.MethodGet {
		writeError(w, r, nethttp.StatusMethodNotAllowed, "method not allowed", h.logger)
		return
	}
	dateParam := r.URL.Query().Get("date")
	games := h.svc.Games()
	date := h.now().Format("2006-01-02")
	logger := loggerFromContext(r, h.logger)
	tz := r.URL.Query().Get("tz")

	if dateParam == "" && tz != "" {
		if loc := providers.ResolveTimezone(tz); loc != nil {
			date = h.now().In(loc).Format("2006-01-02")
		}
	}

	if dateParam != "" {
		if _, err := time.Parse("2006-01-02", dateParam); err != nil {
			writeError(w, r, nethttp.StatusBadRequest, "invalid date format (expected YYYY-MM-DD)", h.logger)
			return
		}
	}

	// For explicit date queries, serve snapshots only (no live upstream fetch).
	if dateParam != "" {
		snap, err := h.loadSnapshot(dateParam)
		if err != nil {
			writeError(w, r, nethttp.StatusBadGateway, "snapshot unavailable", h.logger)
			return
		}
		games = snap.Games
		date = snap.Date
		if logger != nil {
			logger.Info("served snapshot games", "date", date, "provider", "snapshot", "count", len(games))
		}
	} else {
		// Default path: serve cache; if empty, try snapshot for the computed date.
		if len(games) == 0 {
			if snap, err := h.loadSnapshot(date); err == nil {
				games = snap.Games
				date = snap.Date
				if logger != nil {
					logger.Info("served snapshot games", "date", date, "provider", "snapshot", "count", len(games))
				}
			}
		}
		if logger != nil {
			logger.Info("served cached games", "date", date, "provider", "cache", "count", len(games))
		}
	}

	payload := domain.TodayResponse{
		Date:  date,
		Games: games,
	}
	writeJSON(w, nethttp.StatusOK, payload, h.logger)
}

// GameByID returns a specific game if present.
func (h *Handler) GameByID(w nethttp.ResponseWriter, r *nethttp.Request) {
	if r.Method != nethttp.MethodGet {
		writeError(w, r, nethttp.StatusMethodNotAllowed, "method not allowed", h.logger)
		return
	}
	// Expect path: /games/{id}
	path := strings.TrimPrefix(r.URL.Path, "/games")
	if path == "" || path == "/" {
		writeError(w, r, nethttp.StatusBadRequest, "invalid game id", h.logger)
		return
	}

	idRaw := strings.TrimPrefix(path, "/")
	id, err := url.PathUnescape(idRaw)
	if err != nil || id == "" || id == "games" || strings.ContainsAny(id, " \t/") {
		writeError(w, r, nethttp.StatusBadRequest, "invalid game id", h.logger)
		return
	}

	game, ok := h.svc.GameByID(id)
	if !ok {
		writeError(w, r, nethttp.StatusNotFound, "game not found", h.logger)
		return
	}

	writeJSON(w, nethttp.StatusOK, game, h.logger)
}

func (h *Handler) loadSnapshot(date string) (domain.TodayResponse, error) {
	if h.snaps == nil {
		return domain.TodayResponse{}, errors.New("snapshot store not configured")
	}
	ctx := context.Background()
	if err := ctx.Err(); err != nil {
		return domain.TodayResponse{}, err
	}
	return h.snaps.LoadGames(date)
}
