package http

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"nba-games-service/internal/logging"
	"nba-games-service/internal/metrics"
)

// LoggingMiddleware wraps the handler with request logging, request ID support, and metrics.
func LoggingMiddleware(baseLogger *slog.Logger, recorder *metrics.Recorder, next http.Handler) http.Handler {
	if baseLogger == nil {
		baseLogger = slog.Default()
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqID := sanitizeRequestID(r.Header.Get("X-Request-ID"))
		w.Header().Set("X-Request-ID", reqID)

		clientIP := r.RemoteAddr
		if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
			clientIP = forwarded
		}

		logger := baseLogger.With(
			slog.String("request_id", reqID),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("query", r.URL.RawQuery),
			slog.String("client_ip", clientIP),
		)

		ctx := logging.WithLogger(r.Context(), logger)
		ctx = withRequestID(ctx, reqID)
		r = r.WithContext(ctx)
		ww := &responseWriter{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(ww, r)

		duration := time.Since(start)
		if recorder != nil {
			recorder.RecordHTTPRequest(r.Method, normalizePath(r.URL.Path), ww.status, duration)
		}

		logger.Info("request complete",
			slog.Int("status", ww.status),
			slog.Int64("duration_ms", duration.Milliseconds()),
		)
	})
}

func (w *responseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func normalizePath(path string) string {
	if path == "" {
		return ""
	}
	path = strings.Split(path, "?")[0]
	switch path {
	case "/games", "/games/today":
		return "/games/today"
	case "/health":
		return "/health"
	default:
		if strings.HasPrefix(path, "/games/") {
			return "/games/:id"
		}
		return path
	}
}
