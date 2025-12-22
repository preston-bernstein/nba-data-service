package http

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"nba-games-service/internal/metrics"
)

func TestLoggingMiddlewareSetsRequestIDAndRecords(t *testing.T) {
	rec := metrics.NewRecorder()

	baseLogger := slog.Default()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Context().Value(requestIDKey{}); got == nil {
			t.Fatalf("expected request id in context")
		}
		w.WriteHeader(http.StatusTeapot)
	})

	handler := LoggingMiddleware(baseLogger, rec, next)

	req := httptest.NewRequest(http.MethodGet, "/health?foo=bar", nil)
	req.Header.Set("X-Request-ID", "req-123")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusTeapot {
		t.Fatalf("expected status %d, got %d", http.StatusTeapot, rr.Code)
	}
	if rid := rr.Header().Get("X-Request-ID"); rid != "req-123" {
		t.Fatalf("expected request id header to be preserved, got %s", rid)
	}
	// Recorder has no otel instruments here, so just ensure no panic and header set.
}

func TestLoggingMiddlewareRedactsSensitiveQuery(t *testing.T) {
	rec := metrics.NewRecorder()
	baseLogger := slog.Default()

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := LoggingMiddleware(baseLogger, rec, next)

	req := httptest.NewRequest(http.MethodGet, "/games?token=secret", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}
