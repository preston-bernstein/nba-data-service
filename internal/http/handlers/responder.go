package handlers

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/preston-bernstein/nba-data-service/internal/http/middleware"
	"github.com/preston-bernstein/nba-data-service/internal/logging"
)

func writeJSON(w http.ResponseWriter, status int, payload any, logger *slog.Logger) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil && logger != nil {
		logger.Error("failed to encode response", "err", err)
	}
}

func writeError(w http.ResponseWriter, r *http.Request, status int, message string, logger *slog.Logger) {
	reqID := requestID(r)
	body := map[string]string{"error": message}
	if reqID != "" {
		body["requestId"] = reqID
	}
	writeJSON(w, status, body, logger)
}

func loggerFromContext(r *http.Request, fallback *slog.Logger) *slog.Logger {
	if r == nil {
		return fallback
	}
	return logging.FromContext(r.Context(), fallback)
}

func requestID(r *http.Request) string {
	if r == nil {
		return ""
	}
	if id := middleware.RequestIDFromContext(r.Context()); id != "" {
		return id
	}
	return r.Header.Get("X-Request-ID")
}
