package handlers

import (
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/http/requestutil"
	"github.com/preston-bernstein/nba-data-service/internal/logging"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
	"github.com/preston-bernstein/nba-data-service/internal/snapshots"
	"github.com/preston-bernstein/nba-data-service/internal/timeutil"
)

// AdminHandler exposes admin-only endpoints (e.g., snapshot refresh).
type AdminHandler struct {
	writer   *snapshots.Writer
	provider providers.GameProvider
	token    string
	logger   *slog.Logger
}

// NewAdminHandler constructs an AdminHandler.
func NewAdminHandler(writer *snapshots.Writer, provider providers.GameProvider, token string, logger *slog.Logger) *AdminHandler {
	return &AdminHandler{
		writer:   writer,
		provider: provider,
		token:    token,
		logger:   logger,
	}
}

// RefreshSnapshots writes a games snapshot for the requested date (defaults to today).
// Guarded by ADMIN_TOKEN env; returns 401 if missing/invalid.
func (h *AdminHandler) RefreshSnapshots(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost, h.logger) {
		return
	}
	if !h.authorize(r) {
		logging.Warn(h.logger, "admin unauthorized",
			slog.String("path", r.URL.Path),
			slog.String("client_ip", clientIP(r)),
		)
		writeError(w, r, http.StatusUnauthorized, "unauthorized", h.logger)
		return
	}
	if h.provider == nil || h.writer == nil {
		writeError(w, r, http.StatusServiceUnavailable, "snapshot writer not configured", h.logger)
		return
	}

	logger := loggerFromContext(r, h.logger)
	date := strings.TrimSpace(r.URL.Query().Get("date"))
	if date == "" {
		date = timeutil.FormatDate(time.Now())
	}
	// Validate date format.
	if _, err := timeutil.ParseDate(date); err != nil {
		logging.Warn(logger, "admin snapshot invalid date", slog.String("date", date))
		writeError(w, r, http.StatusBadRequest, "invalid date format", logger)
		return
	}
	// Fetch games from provider for the date; no tz support here (keep simple).
	tz := strings.TrimSpace(r.URL.Query().Get("tz"))
	if tz != "" {
		if _, err := time.LoadLocation(tz); err != nil {
			logging.Warn(logger, "admin snapshot invalid tz", slog.String("tz", tz))
			writeError(w, r, http.StatusBadRequest, "invalid timezone", logger)
			return
		}
	}
	games, err := h.provider.FetchGames(r.Context(), date, tz)
	if err != nil {
		logging.Warn(logger, "admin snapshot fetch failed",
			slog.String("date", date),
			slog.String("tz", tz),
			slog.Any("err", err),
		)
		writeError(w, r, http.StatusBadGateway, "failed to fetch games", logger)
		return
	}
	if len(games) == 0 {
		logging.Warn(logger, "admin snapshot no games", slog.String("date", date))
		writeError(w, r, http.StatusBadRequest, "no games to snapshot", logger)
		return
	}

	snap := domaingames.NewTodayResponse(date, games)
	if err := h.writer.WriteGamesSnapshot(date, snap); err != nil {
		logging.Warn(logger, "admin snapshot write failed",
			slog.String("date", date),
			slog.String("tz", tz),
			slog.Int("count", len(games)),
			slog.Any("err", err),
		)
		writeError(w, r, http.StatusInternalServerError, "failed to write snapshot", logger)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"date":      date,
		"snapshots": len(games),
		"status":    "ok",
	}, logger)
	logging.Info(logger, "admin snapshot written",
		slog.String("date", date),
		slog.String("tz", tz),
		slog.Int("count", len(games)),
	)
}

// AdminTokenFromEnv reads ADMIN_TOKEN (optional).
func AdminTokenFromEnv() string {
	return os.Getenv("ADMIN_TOKEN")
}

func (h *AdminHandler) authorize(r *http.Request) bool {
	if h.token == "" {
		return false
	}
	return r.Header.Get("Authorization") == "Bearer "+h.token
}

func clientIP(r *http.Request) string {
	return requestutil.ClientIP(r)
}
