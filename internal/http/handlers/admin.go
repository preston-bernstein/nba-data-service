package handlers

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/gamedate"
	"github.com/preston-bernstein/nba-data-service/internal/http/requestmeta"
	"github.com/preston-bernstein/nba-data-service/internal/logging"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
	"github.com/preston-bernstein/nba-data-service/internal/snapshots"
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

// refreshRequest is the validated query of a snapshot refresh call.
type refreshRequest struct {
	date string
	tz   string
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
		writeError(w, r, apiError{status: http.StatusUnauthorized, message: "unauthorized"}, h.logger)
		return
	}
	if h.provider == nil || h.writer == nil {
		writeError(w, r, apiError{status: http.StatusServiceUnavailable, message: "snapshot writer not configured"}, h.logger)
		return
	}

	logger := loggerFromContext(r, h.logger)
	req, apiErr := parseRefreshRequest(r, logger)
	if apiErr != nil {
		writeError(w, r, *apiErr, logger)
		return
	}
	games, apiErr := h.fetchGames(r, req, logger)
	if apiErr != nil {
		writeError(w, r, *apiErr, logger)
		return
	}
	if apiErr := h.writeSnapshot(req, games, logger); apiErr != nil {
		writeError(w, r, *apiErr, logger)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"date":      req.date,
		"snapshots": len(games),
		"status":    "ok",
	}, logger)
	logging.Info(logger, "admin snapshot written",
		slog.String("date", req.date),
		slog.String("tz", req.tz),
		slog.Int("count", len(games)),
	)
}

// parseRefreshRequest reads and validates the date and tz query params.
// The tz is only validated here; the provider applies it.
func parseRefreshRequest(r *http.Request, logger *slog.Logger) (refreshRequest, *apiError) {
	date := strings.TrimSpace(r.URL.Query().Get("date"))
	if date == "" {
		date = gamedate.FormatDate(time.Now())
	}
	if _, err := gamedate.ParseDate(date); err != nil {
		logging.Warn(logger, "admin snapshot invalid date", slog.String("date", date))
		return refreshRequest{}, &apiError{status: http.StatusBadRequest, message: "invalid date format"}
	}
	tz := strings.TrimSpace(r.URL.Query().Get("tz"))
	if tz != "" {
		if _, err := time.LoadLocation(tz); err != nil {
			logging.Warn(logger, "admin snapshot invalid tz", slog.String("tz", tz))
			return refreshRequest{}, &apiError{status: http.StatusBadRequest, message: "invalid timezone"}
		}
	}
	return refreshRequest{date: date, tz: tz}, nil
}

func (h *AdminHandler) fetchGames(r *http.Request, req refreshRequest, logger *slog.Logger) ([]domaingames.Game, *apiError) {
	games, err := h.provider.FetchGames(r.Context(), req.date, req.tz)
	if err != nil {
		logging.Warn(logger, "admin snapshot fetch failed",
			slog.String("date", req.date),
			slog.String("tz", req.tz),
			slog.Any("err", err),
		)
		return nil, &apiError{status: http.StatusBadGateway, message: "failed to fetch games"}
	}
	if len(games) == 0 {
		logging.Warn(logger, "admin snapshot no games", slog.String("date", req.date))
		return nil, &apiError{status: http.StatusBadRequest, message: "no games to snapshot"}
	}
	return games, nil
}

func (h *AdminHandler) writeSnapshot(req refreshRequest, games []domaingames.Game, logger *slog.Logger) *apiError {
	snap := domaingames.NewTodayResponse(req.date, games)
	if err := h.writer.WriteGamesSnapshot(req.date, snap); err != nil {
		logging.Warn(logger, "admin snapshot write failed",
			slog.String("date", req.date),
			slog.String("tz", req.tz),
			slog.Int("count", len(games)),
			slog.Any("err", err),
		)
		return &apiError{status: http.StatusInternalServerError, message: "failed to write snapshot"}
	}
	return nil
}

func (h *AdminHandler) authorize(r *http.Request) bool {
	if h.token == "" {
		return false
	}
	return r.Header.Get("Authorization") == "Bearer "+h.token
}

func clientIP(r *http.Request) string {
	return requestmeta.ClientIP(r)
}
