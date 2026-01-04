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
	appplayers "github.com/preston-bernstein/nba-data-service/internal/app/players"
	appteams "github.com/preston-bernstein/nba-data-service/internal/app/teams"
	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/poller"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
	"github.com/preston-bernstein/nba-data-service/internal/snapshots"
)

type nowFunc func() time.Time

// Handler wires HTTP routes to the domain service.
type Handler struct {
	gamesSvc   *games.Service
	teamsSvc   *appteams.Service
	playersSvc *appplayers.Service
	snaps      snapshots.Store
	logger     *slog.Logger
	now        nowFunc
	statusFn   func() poller.Status
}

// NewHandler constructs a Handler with defaults.
func NewHandler(gameSvc *games.Service, teamSvc *appteams.Service, playerSvc *appplayers.Service, snaps snapshots.Store, logger *slog.Logger, statusFn func() poller.Status) *Handler {
	return &Handler{
		gamesSvc:   gameSvc,
		teamsSvc:   teamSvc,
		playersSvc: playerSvc,
		snaps:      snaps,
		logger:     logger,
		now:        time.Now,
		statusFn:   statusFn,
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
	case r.URL.Path == "/teams":
		h.Teams(w, r)
	case strings.HasPrefix(r.URL.Path, "/teams/"):
		h.TeamByID(w, r)
	case r.URL.Path == "/players":
		h.Players(w, r)
	case strings.HasPrefix(r.URL.Path, "/players/"):
		h.PlayerByID(w, r)
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
	games := h.gamesSvc.Games()
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

	payload := domaingames.TodayResponse{
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

	game, ok := h.gamesSvc.GameByID(id)
	if !ok {
		writeError(w, r, nethttp.StatusNotFound, "game not found", h.logger)
		return
	}

	writeJSON(w, nethttp.StatusOK, game, h.logger)
}

// Teams returns all cached teams.
func (h *Handler) Teams(w nethttp.ResponseWriter, r *nethttp.Request) {
	if r.Method != nethttp.MethodGet {
		writeError(w, r, nethttp.StatusMethodNotAllowed, "method not allowed", h.logger)
		return
	}
	if h.teamsSvc == nil {
		writeError(w, r, nethttp.StatusServiceUnavailable, "teams unavailable", h.logger)
		return
	}
	activeOnly := isActiveOnly(r)
	teams := h.teamsSvc.Teams()
	if activeOnly {
		teams = h.teamsSvc.ActiveTeams()
	}
	writeJSON(w, nethttp.StatusOK, teams, h.logger)
}

// TeamByID returns a team by id.
func (h *Handler) TeamByID(w nethttp.ResponseWriter, r *nethttp.Request) {
	if r.Method != nethttp.MethodGet {
		writeError(w, r, nethttp.StatusMethodNotAllowed, "method not allowed", h.logger)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/teams/")
	if id == "" {
		writeError(w, r, nethttp.StatusBadRequest, "invalid team id", h.logger)
		return
	}
	if h.teamsSvc == nil {
		writeError(w, r, nethttp.StatusServiceUnavailable, "teams unavailable", h.logger)
		return
	}
	team, ok := h.teamsSvc.TeamByID(id)
	if !ok {
		writeError(w, r, nethttp.StatusNotFound, "team not found", h.logger)
		return
	}
	writeJSON(w, nethttp.StatusOK, team, h.logger)
}

// Players returns all cached players.
func (h *Handler) Players(w nethttp.ResponseWriter, r *nethttp.Request) {
	if r.Method != nethttp.MethodGet {
		writeError(w, r, nethttp.StatusMethodNotAllowed, "method not allowed", h.logger)
		return
	}
	if h.playersSvc == nil {
		writeError(w, r, nethttp.StatusServiceUnavailable, "players unavailable", h.logger)
		return
	}
	activeOnly := isActiveOnly(r)
	players := h.playersSvc.Players()
	if activeOnly {
		players = h.playersSvc.ActivePlayers()
	}
	writeJSON(w, nethttp.StatusOK, players, h.logger)
}

// PlayerByID returns a player by id.
func (h *Handler) PlayerByID(w nethttp.ResponseWriter, r *nethttp.Request) {
	if r.Method != nethttp.MethodGet {
		writeError(w, r, nethttp.StatusMethodNotAllowed, "method not allowed", h.logger)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/players/")
	if id == "" {
		writeError(w, r, nethttp.StatusBadRequest, "invalid player id", h.logger)
		return
	}
	if h.playersSvc == nil {
		writeError(w, r, nethttp.StatusServiceUnavailable, "players unavailable", h.logger)
		return
	}
	player, ok := h.playersSvc.PlayerByID(id)
	if !ok {
		writeError(w, r, nethttp.StatusNotFound, "player not found", h.logger)
		return
	}
	writeJSON(w, nethttp.StatusOK, player, h.logger)
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
