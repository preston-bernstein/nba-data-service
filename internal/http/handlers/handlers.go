package handlers

import (
	"context"
	"errors"
	"log/slog"
	nethttp "net/http"
	"net/url"
	"strings"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/poller"
	"github.com/preston-bernstein/nba-data-service/internal/snapshots"
	"github.com/preston-bernstein/nba-data-service/internal/timeutil"
)

type nowFunc func() time.Time

// Handler wires HTTP routes to the snapshot store.
type Handler struct {
	snaps    snapshots.Store
	logger   *slog.Logger
	now      nowFunc
	statusFn func() poller.Status
}

// NewHandler constructs a Handler with defaults.
func NewHandler(snaps snapshots.Store, logger *slog.Logger, statusFn func() poller.Status) *Handler {
	return &Handler{
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
	case r.URL.Path == "/games":
		h.GamesToday(w, r)
	case strings.HasPrefix(r.URL.Path, "/games/"):
		h.GameByID(w, r)
	default:
		writeError(w, r, nethttp.StatusNotFound, "not found", h.logger)
	}
}

func (h *Handler) Health(w nethttp.ResponseWriter, r *nethttp.Request) {
	if !requireMethod(w, r, nethttp.MethodGet, h.logger) {
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
	if !requireMethod(w, r, nethttp.MethodGet, h.logger) {
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

// GamesToday returns the snapshot of games for a requested date.
func (h *Handler) GamesToday(w nethttp.ResponseWriter, r *nethttp.Request) {
	if !requireMethod(w, r, nethttp.MethodGet, h.logger) {
		return
	}
	dateParam := r.URL.Query().Get("date")
	if dateParam == "" {
		writeError(w, r, nethttp.StatusBadRequest, "date query param required (expected YYYY-MM-DD)", h.logger)
		return
	}
	now := h.now().UTC()
	logger := loggerFromContext(r, h.logger)

	parsed, err := timeutil.ParseDate(dateParam)
	if err != nil {
		writeError(w, r, nethttp.StatusBadRequest, "invalid date format (expected YYYY-MM-DD)", h.logger)
		return
	}
	today := now.Truncate(24 * time.Hour)
	minDate := today.AddDate(0, 0, -7)
	maxDate := today.AddDate(0, 0, 7)
	if parsed.Before(minDate) || parsed.After(maxDate) {
		writeError(w, r, nethttp.StatusBadRequest, "date must be within 7 days of today", h.logger)
		return
	}

	snap, err := h.loadSnapshot(dateParam)
	if err != nil {
		writeError(w, r, nethttp.StatusBadGateway, "snapshot unavailable", h.logger)
		return
	}
	if logger != nil {
		logger.Info("served snapshot games", "date", snap.Date, "provider", "snapshot", "count", len(snap.Games))
	}

	payload := domaingames.NewTodayResponse(snap.Date, snap.Games)
	writeJSON(w, nethttp.StatusOK, payload, h.logger)
}

// GameByID returns a specific game if present in today's snapshot.
func (h *Handler) GameByID(w nethttp.ResponseWriter, r *nethttp.Request) {
	if !requireMethod(w, r, nethttp.MethodGet, h.logger) {
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

	if h.snaps == nil {
		writeError(w, r, nethttp.StatusBadGateway, "snapshot store not configured", h.logger)
		return
	}
	today := timeutil.FormatDate(h.now().UTC())
	game, ok := h.snaps.FindGameByID(today, id)
	if !ok {
		writeError(w, r, nethttp.StatusNotFound, "game not found", h.logger)
		return
	}

	writeJSON(w, nethttp.StatusOK, game, h.logger)
}

func (h *Handler) loadSnapshot(date string) (domaingames.TodayResponse, error) {
	if h.snaps == nil {
		return domaingames.TodayResponse{}, errors.New("snapshot store not configured")
	}
	ctx := context.Background()
	if err := ctx.Err(); err != nil {
		return domaingames.TodayResponse{}, err
	}
	return h.snaps.LoadGames(date)
}
