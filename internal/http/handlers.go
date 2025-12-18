package http

import (
	"encoding/json"
	"log/slog"
	nethttp "net/http"
	"strings"
	"time"

	"nba-games-service/internal/domain"
	"nba-games-service/internal/logging"
	"nba-games-service/internal/providers"
)

type nowFunc func() time.Time

// Handler wires HTTP routes to the domain service.
type Handler struct {
	svc      *domain.Service
	logger   *slog.Logger
	now      nowFunc
	provider providers.GameProvider
}

// NewHandler constructs a Handler with defaults.
func NewHandler(svc *domain.Service, logger *slog.Logger, provider providers.GameProvider) *Handler {
	return &Handler{
		svc:      svc,
		logger:   logger,
		now:      time.Now,
		provider: provider,
	}
}

// Health reports the service health.
func (h *Handler) Health(w nethttp.ResponseWriter, r *nethttp.Request) {
	resp := map[string]string{"status": "ok"}
	h.writeJSON(w, nethttp.StatusOK, resp)
}

// GamesToday returns the current snapshot of games.
func (h *Handler) GamesToday(w nethttp.ResponseWriter, r *nethttp.Request) {
	dateParam := r.URL.Query().Get("date")
	games := h.svc.Games()
	date := h.now().Format("2006-01-02")
	logger := logging.FromContext(r.Context(), h.logger)
	tz := r.URL.Query().Get("tz")

	if dateParam == "" && tz != "" {
		if loc := providers.ResolveTimezone(tz); loc != nil {
			date = h.now().In(loc).Format("2006-01-02")
		}
	}

	if dateParam != "" && h.provider != nil {
		fetched, err := h.provider.FetchGames(r.Context(), dateParam, tz)
		if err != nil {
			if logger != nil {
				logger.Warn("failed to fetch games", "date", dateParam, "err", err)
			}
			h.writeError(w, r, nethttp.StatusBadGateway, "failed to fetch games")
			return
		}
		games = fetched
		date = dateParam
		if logger != nil {
			logger.Info("fetched games", "date", date, "provider", "external", "count", len(games))
		}
	} else {
		if logger != nil {
			logger.Info("served cached games", "date", date, "provider", "cache", "count", len(games))
		}
	}

	payload := domain.TodayResponse{
		Date:  date,
		Games: games,
	}
	h.writeJSON(w, nethttp.StatusOK, payload)
}

// GameByID returns a specific game if present.
func (h *Handler) GameByID(w nethttp.ResponseWriter, r *nethttp.Request) {
	// Expect path: /games/{id}
	id := strings.TrimPrefix(r.URL.Path, "/games/")
	if id == "" || id == "games" {
		h.writeError(w, r, nethttp.StatusBadRequest, "missing game id")
		return
	}

	game, ok := h.svc.GameByID(id)
	if !ok {
		h.writeError(w, r, nethttp.StatusNotFound, "game not found")
		return
	}

	h.writeJSON(w, nethttp.StatusOK, game)
}

func (h *Handler) writeJSON(w nethttp.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil && h.logger != nil {
		h.logger.Error("failed to encode response", "err", err)
	}
}

func (h *Handler) writeError(w nethttp.ResponseWriter, r *nethttp.Request, status int, message string) {
	reqID := requestIDFromContext(r.Context())
	body := map[string]string{"error": message}
	if reqID != "" {
		body["requestId"] = reqID
	}
	h.writeJSON(w, status, body)
}
