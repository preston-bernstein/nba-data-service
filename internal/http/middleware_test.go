package http

import "testing"

func TestNormalizePath(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"/health", "/health"},
		{"/games", "/games/today"},
		{"/games/today", "/games/today"},
		{"/games/123", "/games/:id"},
		{"/games/123?foo=bar", "/games/:id"},
		{"/other", "/other"},
		{"", ""},
	}

	for _, c := range cases {
		if got := normalizePath(c.in); got != c.want {
			t.Fatalf("normalizePath(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
