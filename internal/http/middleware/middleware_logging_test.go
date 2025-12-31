package middleware

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"nba-data-service/internal/metrics"
)

func TestLoggingMiddlewareSetsRequestIDAndRecordsMetrics(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	rec := metrics.NewRecorder()
	nextCalled := false

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		if got := RequestIDFromContext(r.Context()); got == "" {
			t.Fatalf("expected request id in context")
		}
		w.WriteHeader(http.StatusTeapot)
	})

	req := httptest.NewRequest(http.MethodGet, "/games/today", nil)
	rr := httptest.NewRecorder()

	handler := LoggingMiddleware(logger, rec, next)
	handler.ServeHTTP(rr, req)

	if !nextCalled {
		t.Fatalf("expected next handler to be called")
	}
	if rr.Code != http.StatusTeapot {
		t.Fatalf("expected status 418, got %d", rr.Code)
	}
	if rec.ProviderCalls("http") != 0 {
		t.Fatalf("expected provider metrics untouched")
	}
	if got := rec.Snapshot("http").Calls; got != 0 {
		t.Fatalf("expected no provider metrics recorded, got %d", got)
	}
}

func TestNormalizePath(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "/games", want: "/games/today"},
		{in: "/games/today", want: "/games/today"},
		{in: "/games/123", want: "/games/:id"},
		{in: "/health", want: "/health"},
		{in: "/ready", want: "/ready"},
	}

	for _, tt := range tests {
		if got := normalizePath(tt.in); got != tt.want {
			t.Fatalf("normalizePath(%s) = %s, want %s", tt.in, got, tt.want)
		}
	}
}

func TestRequestIDHelpers(t *testing.T) {
	ctx := context.Background()
	if got := RequestIDFromContext(ctx); got != "" {
		t.Fatalf("expected empty id, got %s", got)
	}

	ctx = withRequestID(ctx, "abc123")
	if got := RequestIDFromContext(ctx); got != "abc123" {
		t.Fatalf("expected id from context, got %s", got)
	}
}

func TestRequestIDSanitization(t *testing.T) {
	if generateRequestID() == "" {
		t.Fatalf("expected generated id")
	}
	if fallbackRequestID() == "" {
		t.Fatalf("expected fallback id")
	}
	if sanitizeRequestID("valid-123") != "valid-123" {
		t.Fatalf("expected valid id to pass through")
	}
	sanitized := sanitizeRequestID("bad id")
	if sanitized == "bad id" || sanitized == "" {
		t.Fatalf("expected sanitized id to differ and be non-empty")
	}
}

func BenchmarkLoggingMiddleware(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	rec := metrics.NewRecorder()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Microsecond)
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/games/today", nil)
	rr := httptest.NewRecorder()

	handler := LoggingMiddleware(logger, rec, next)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(rr, req)
	}
}
