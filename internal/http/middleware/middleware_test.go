package middleware

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/preston-bernstein/nba-data-service/internal/metrics"
	"github.com/preston-bernstein/nba-data-service/internal/testutil"
)

func TestLoggingMiddlewareSetsRequestIDAndRecordsMetrics(t *testing.T) {
	logger, _ := testutil.NewBufferLogger()
	rec := metrics.NewRecorder()
	nextCalled := false

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		if got := RequestIDFromContext(r.Context()); got == "" {
			t.Fatalf("expected request id in context")
		}
		w.WriteHeader(http.StatusTeapot)
	})

	handler := LoggingMiddleware(logger, rec, next)
	rr := testutil.Serve(handler, http.MethodGet, "/games/today", nil)

	if !nextCalled {
		t.Fatalf("expected next handler to be called")
	}
	testutil.AssertStatus(t, rr, http.StatusTeapot)
	if rec.ProviderCalls("http") != 0 {
		t.Fatalf("expected provider metrics untouched")
	}
	if got := rec.Snapshot("http").Calls; got != 0 {
		t.Fatalf("expected no provider metrics recorded, got %d", got)
	}
}

func TestLoggingMiddlewareGeneratesRequestIDWhenMissing(t *testing.T) {
	logger, _ := testutil.NewBufferLogger()
	rec := metrics.NewRecorder()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := RequestIDFromContext(r.Context()); got == "" {
			t.Fatalf("expected generated request id")
		}
		w.WriteHeader(http.StatusOK)
	})

	handler := LoggingMiddleware(logger, rec, next)
	rr := testutil.Serve(handler, http.MethodGet, "/games/today?foo=bar", nil)

	testutil.AssertStatus(t, rr, http.StatusOK)
	if got := rr.Header().Get("X-Request-ID"); got == "" {
		t.Fatalf("expected X-Request-ID header to be set")
	}
}

func TestLoggingMiddlewareUsesForwardedFor(t *testing.T) {
	logger, _ := testutil.NewBufferLogger()
	rec := metrics.NewRecorder()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := LoggingMiddleware(logger, rec, next)
	req := httptest.NewRequest(http.MethodGet, "/games/today", nil)
	req.Header.Set("X-Forwarded-For", "198.51.100.1")
	rr := testutil.ServeRequest(handler, req)

	testutil.AssertStatus(t, rr, http.StatusOK)
}

// Ensure responseWriter defaults status correctly.
func TestResponseWriterDefaultsStatus(t *testing.T) {
	rr := httptest.NewRecorder()
	w := &responseWriter{ResponseWriter: rr}
	if w.status != 0 {
		t.Fatalf("expected zero status before write, got %d", w.status)
	}
	w.WriteHeader(http.StatusAccepted)
	if w.status != http.StatusAccepted {
		t.Fatalf("expected status set to 202, got %d", w.status)
	}
}

func TestNormalizePathHandlesEmpty(t *testing.T) {
	if got := normalizePath(""); got != "" {
		t.Fatalf("expected empty path to stay empty, got %s", got)
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

func TestRequestIDFromContextEmpty(t *testing.T) {
	if got := RequestIDFromContext(nil); got != "" {
		t.Fatalf("expected empty id for nil context, got %s", got)
	}
}

func BenchmarkLoggingMiddleware(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	rec := metrics.NewRecorder()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Microsecond)
		w.WriteHeader(http.StatusOK)
	})

	handler := LoggingMiddleware(logger, rec, next)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/games/today", nil)
		handler.ServeHTTP(rr, req)
	}
}
