package balldontlie

import (
	"net/http"
	"strings"
	"time"
)

type httpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

func resolveHTTPClient(client *http.Client) httpDoer {
	if client != nil {
		return client
	}
	return &http.Client{Timeout: defaultHTTPTimeout}
}

func normalizeBaseURL(raw string) string {
	if raw == "" {
		raw = defaultBaseURL
	}
	return strings.TrimSuffix(raw, "/")
}

func resolveLocation(name string) *time.Location {
	if name == "" {
		name = defaultTimezone
	}
	if loc, err := time.LoadLocation(name); err == nil {
		return loc
	}
	return time.UTC
}

func resolveMaxPages(max int) int {
	if max <= 0 {
		return defaultMaxPages
	}
	return max
}
