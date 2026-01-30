package middleware

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/preston-bernstein/nba-data-service/internal/http/requestutil"
	"github.com/preston-bernstein/nba-data-service/internal/logging"
	"github.com/preston-bernstein/nba-data-service/internal/metrics"
)

// LoggingMiddleware wraps the handler with request logging, request ID support, and metrics.
func LoggingMiddleware(baseLogger *slog.Logger, recorder *metrics.Recorder, next http.Handler) http.Handler {
	if baseLogger == nil {
		baseLogger = slog.Default()
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqID := requestutil.SanitizeRequestID(r.Header.Get("X-Request-ID"))
		w.Header().Set("X-Request-ID", reqID)

		clientIP := requestutil.ClientIP(r)

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
			slog.Int(logging.FieldStatusCode, ww.status),
			slog.Int64(logging.FieldDurationMS, duration.Milliseconds()),
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

// RequestIDFromContext extracts the request ID stored by the logging middleware.
func RequestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if val, ok := ctx.Value(requestIDKey{}).(string); ok {
		return val
	}
	return ""
}

func withRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

type requestIDKey struct{}

func normalizePath(path string) string {
	if path == "" {
		return ""
	}
	path = strings.Split(path, "?")[0]
	switch path {
	case "/games":
		return "/games"
	case "/health":
		return "/health"
	default:
		if strings.HasPrefix(path, "/games/") {
			return "/games/:id"
		}
		return path
	}
}
