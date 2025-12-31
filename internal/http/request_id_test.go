package http

import (
	"context"
	"testing"
)

func TestRequestIDGenerationAndSanitization(t *testing.T) {
	id := generateRequestID()
	if id == "" {
		t.Fatalf("expected generated id")
	}
	if fb := fallbackRequestID(); fb == "" {
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

func TestRequestIDContextHelpers(t *testing.T) {
	ctx := context.Background()
	if got := requestIDFromContext(ctx); got != "" {
		t.Fatalf("expected empty id, got %s", got)
	}

	ctx = withRequestID(ctx, "abc123")
	if got := requestIDFromContext(ctx); got != "abc123" {
		t.Fatalf("expected id from context, got %s", got)
	}
}
