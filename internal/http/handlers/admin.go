package handlers

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"nba-data-service/internal/app/games"
	"nba-data-service/internal/domain"
	"nba-data-service/internal/logging"
	"nba-data-service/internal/providers"
	"nba-data-service/internal/snapshots"
)

// AdminHandler exposes admin-only endpoints (e.g., snapshot refresh).
type AdminHandler struct {
	app      *games.Service
	writer   *snapshots.Writer
	provider providers.GameProvider
	token    string
	logger   *slog.Logger
}

// NewAdminHandler constructs an AdminHandler.
func NewAdminHandler(app *games.Service, writer *snapshots.Writer, provider providers.GameProvider, token string, logger *slog.Logger) *AdminHandler {
	return &AdminHandler{
		app:      app,
		writer:   writer,
		provider: provider,
		token:    token,
		logger:   logger,
	}
}

// RefreshSnapshots writes a games snapshot for the requested date (defaults to today).
// Guarded by ADMIN_TOKEN env; returns 401 if missing/invalid.
func (h *AdminHandler) RefreshSnapshots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.token == "" || r.Header.Get("Authorization") != "Bearer "+h.token {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if h.provider == nil || h.writer == nil {
		http.Error(w, "snapshot writer not configured", http.StatusServiceUnavailable)
		return
	}

	logger := logging.FromContext(r.Context(), h.logger)
	date := strings.TrimSpace(r.URL.Query().Get("date"))
	if date == "" {
		date = time.Now().Format("2006-01-02")
	}
	// Validate date format.
	if _, err := time.Parse("2006-01-02", date); err != nil {
		if logger != nil {
			logger.Warn("admin snapshot invalid date", slog.String("date", date))
		}
		http.Error(w, "invalid date format", http.StatusBadRequest)
		return
	}
	// Fetch games from provider for the date; no tz support here (keep simple).
	tz := strings.TrimSpace(r.URL.Query().Get("tz"))
	if tz != "" {
		if _, err := time.LoadLocation(tz); err != nil {
			if logger != nil {
				logger.Warn("admin snapshot invalid tz", slog.String("tz", tz))
			}
			http.Error(w, "invalid timezone", http.StatusBadRequest)
			return
		}
	}
	games, err := h.provider.FetchGames(r.Context(), date, tz)
	if err != nil {
		if logger != nil {
			logger.Warn("admin snapshot fetch failed", slog.String("date", date), slog.String("tz", tz), slog.Any("err", err))
		}
		http.Error(w, "failed to fetch games", http.StatusBadGateway)
		return
	}
	if len(games) == 0 {
		if logger != nil {
			logger.Warn("admin snapshot no games", slog.String("date", date))
		}
		http.Error(w, "no games to snapshot", http.StatusBadRequest)
		return
	}

	snap := domain.TodayResponse{
		Date:  date,
		Games: games,
	}
	if err := h.writer.WriteGamesSnapshot(date, snap); err != nil {
		if logger != nil {
			logger.Warn("admin snapshot write failed", slog.String("date", date), slog.Any("err", err))
		}
		http.Error(w, "failed to write snapshot", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"date":      date,
		"snapshots": len(games),
		"status":    "ok",
	})
	if logger != nil {
		logger.Info("admin snapshot written", slog.String("date", date), slog.Int("count", len(games)))
	}
}

// AdminTokenFromEnv reads ADMIN_TOKEN (optional).
func AdminTokenFromEnv() string {
	return os.Getenv("ADMIN_TOKEN")
}
