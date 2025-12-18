package balldontlie

import (
	"net/http"
	"testing"
	"time"
)

func TestNormalizeBaseURLTrimsTrailingSlashAndDefaults(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{"", defaultBaseURL},
		{"https://api.example.com/", "https://api.example.com"},
		{"https://api.example.com", "https://api.example.com"},
	}

	for _, c := range cases {
		if got := normalizeBaseURL(c.input); got != c.expected {
			t.Fatalf("expected %s, got %s", c.expected, got)
		}
	}
}

func TestResolveHTTPClientDefaultsTimeout(t *testing.T) {
	client := resolveHTTPClient(nil)
	httpClient, ok := client.(*http.Client)
	if !ok {
		t.Fatalf("expected *http.Client, got %T", client)
	}
	if httpClient.Timeout != defaultHTTPTimeout {
		t.Fatalf("expected timeout %s, got %s", defaultHTTPTimeout, httpClient.Timeout)
	}
}

func TestResolveHTTPClientUsesProvidedClient(t *testing.T) {
	custom := &http.Client{Timeout: 5 * time.Second}
	client := resolveHTTPClient(custom)
	if client != custom {
		t.Fatalf("expected provided client to be used")
	}
}

func TestResolveLocationDefaultsAndFallback(t *testing.T) {
	loc := resolveLocation("")
	if loc.String() != defaultTimezone {
		t.Fatalf("expected default timezone %s, got %s", defaultTimezone, loc.String())
	}

	utc := resolveLocation("UTC")
	if utc.String() != "UTC" {
		t.Fatalf("expected UTC, got %s", utc.String())
	}

	if fallback := resolveLocation("Not/AZone"); fallback.String() != "UTC" {
		t.Fatalf("expected fallback to UTC, got %s", fallback.String())
	}
}

func TestResolveMaxPages(t *testing.T) {
	if got := resolveMaxPages(0); got != defaultMaxPages {
		t.Fatalf("expected default max pages %d, got %d", defaultMaxPages, got)
	}
	if got := resolveMaxPages(3); got != 3 {
		t.Fatalf("expected max pages 3, got %d", got)
	}
}
