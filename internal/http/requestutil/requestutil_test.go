package requestutil

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSanitizeRequestID(t *testing.T) {
	if got := SanitizeRequestID("valid-123"); got != "valid-123" {
		t.Fatalf("expected pass-through, got %s", got)
	}
	if got := SanitizeRequestID("bad id"); got == "" || got == "bad id" {
		t.Fatalf("expected sanitized id, got %s", got)
	}
	if got := NewRequestID(); got == "" {
		t.Fatalf("expected generated request id")
	}
	useFallback.Store(true)
	defer useFallback.Store(false)
	if got := NewRequestID(); got == "" {
		t.Fatalf("expected fallback request id when RNG fails")
	}
}

func TestClientIP(t *testing.T) {
	if got := ClientIP(nil); got != "" {
		t.Fatalf("expected empty for nil request, got %q", got)
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Forwarded-For", "1.2.3.4, 5.6.7.8")
	if got := ClientIP(req); got != "1.2.3.4" {
		t.Fatalf("expected first forwarded address, got %s", got)
	}

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "9.9.9.9:1234"
	if got := ClientIP(req); got != "9.9.9.9:1234" {
		t.Fatalf("expected remote addr fallback, got %s", got)
	}
}
