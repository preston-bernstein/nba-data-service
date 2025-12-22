package balldontlie

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"nba-games-service/internal/providers"
)

func TestFetchGamesHitsAPIAndMapsResponse(t *testing.T) {
	fixed := time.Date(2024, 1, 2, 3, 0, 0, 0, time.UTC) // should still yield 2024-01-01 in America/New_York
	var capturedAuth string
	var capturedQueries []string

	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/games" {
			t.Fatalf("expected /games path, got %s", req.URL.Path)
		}
		capturedQueries = append(capturedQueries, req.URL.RawQuery)
		capturedAuth = req.Header.Get("Authorization")

		body := `{
			"data": [
				{
					"id": 10,
					"date": "2024-01-02T15:00:00Z",
					"status": "Final",
					"home_team": { "id": 1, "full_name": "Home Team" },
					"visitor_team": { "id": 2, "full_name": "Away Team" },
					"home_team_score": 110,
					"visitor_team_score": 102,
					"season": 2023
				}
			]
		}`
		if len(capturedQueries) == 1 {
			body = `{
				"data": [
					{
						"id": 10,
						"date": "2024-01-02T15:00:00Z",
						"status": "Final",
						"home_team": { "id": 1, "full_name": "Home Team" },
						"visitor_team": { "id": 2, "full_name": "Away Team" },
						"home_team_score": 110,
						"visitor_team_score": 102,
						"season": 2023
					}
				],
				"meta": {
					"total_pages": 2
				}
			}`
		} else {
			body = `{
				"data": [
					{
						"id": 11,
						"date": "2024-01-03T15:00:00Z",
						"status": "Final",
						"home_team": { "id": 3, "full_name": "Another Team" },
						"visitor_team": { "id": 4, "full_name": "Away Team 2" },
						"home_team_score": 120,
						"visitor_team_score": 115,
						"season": 2023
					}
				],
				"meta": {
					"total_pages": 2
				}
			}`
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
		}, nil
	})

	client := NewClient(Config{
		BaseURL:    "http://example.com",
		APIKey:     "secret",
		HTTPClient: &http.Client{Transport: rt},
		Timezone:   "America/New_York",
		MaxPages:   2,
	})
	client.now = func() time.Time { return fixed }

	games, err := client.FetchGames(context.Background(), "", "")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if capturedAuth != "Bearer secret" {
		t.Fatalf("expected authorization header, got %s", capturedAuth)
	}
	if len(capturedQueries) != 2 {
		t.Fatalf("expected 2 requests (pagination), got %d", len(capturedQueries))
	}
	q, err := url.ParseQuery(capturedQueries[0])
	if err != nil {
		t.Fatalf("failed parsing query %s: %v", capturedQueries[0], err)
	}
	if q.Get("per_page") != "100" {
		t.Fatalf("expected per_page=100, got %s", q.Get("per_page"))
	}
	if q.Get("dates[]") != "2024-01-01" {
		t.Fatalf("expected date=2024-01-01 in NY, got %s", q.Get("dates[]"))
	}
	if q.Get("page") != "1" {
		t.Fatalf("expected page=1, got %s", q.Get("page"))
	}
	if len(games) != 2 {
		t.Fatalf("expected games from both pages, got %d", len(games))
	}

	game := games[0]
	if game.ID != "balldontlie-10" || game.Provider != "balldontlie" {
		t.Fatalf("unexpected game identifiers %+v", game)
	}
	if game.Score.Home != 110 || game.Score.Away != 102 {
		t.Fatalf("unexpected scores %+v", game.Score)
	}
	if game.Status != "FINAL" {
		t.Fatalf("unexpected status %s", game.Status)
	}
	if game.Meta.UpstreamGameID != 10 || game.Meta.Season != "2023" {
		t.Fatalf("unexpected meta %+v", game.Meta)
	}
}

func TestFetchGamesHandlesNon200(t *testing.T) {
	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		_ = req
		return &http.Response{
			StatusCode: http.StatusBadGateway,
			Body:       io.NopCloser(strings.NewReader("boom")),
			Header:     make(http.Header),
		}, nil
	})

	client := NewClient(Config{
		BaseURL:    "http://example.com",
		HTTPClient: &http.Client{Transport: rt},
	})
	client.now = func() time.Time { return time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC) }

	if _, err := client.FetchGames(context.Background(), "", ""); err == nil {
		t.Fatal("expected error on non-200 response")
	}
}

func TestFetchGamesHandlesDecodeError(t *testing.T) {
	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		_ = req
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("{bad json")),
			Header:     make(http.Header),
		}, nil
	})

	client := NewClient(Config{
		BaseURL:    "http://example.com",
		HTTPClient: &http.Client{Transport: rt},
	})
	client.now = func() time.Time { return time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC) }

	if _, err := client.FetchGames(context.Background(), "", ""); err == nil {
		t.Fatal("expected decode error")
	}
}

func TestFetchGamesHandlesRateLimit(t *testing.T) {
	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		_ = req
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Body:       io.NopCloser(strings.NewReader("slow down")),
			Header: http.Header{
				"Retry-After":             []string{"10"},
				"X-Rate-Limit-Remaining":  []string{"0"},
				"X-Another-Rate-Limit-Ty": []string{"unused"},
			},
		}, nil
	})

	client := NewClient(Config{
		BaseURL:    "http://example.com",
		HTTPClient: &http.Client{Transport: rt},
	})
	client.now = func() time.Time { return time.Unix(0, 0) }

	_, err := client.FetchGames(context.Background(), "", "")
	if err == nil {
		t.Fatal("expected rate limit error")
	}

	rlErr, ok := err.(*providers.RateLimitError)
	if !ok {
		t.Fatalf("expected RateLimitError, got %T", err)
	}
	if rlErr.RetryAfter != 10*time.Second {
		t.Fatalf("expected retry-after 10s, got %s", rlErr.RetryAfter)
	}
	if rlErr.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("unexpected status code %d", rlErr.StatusCode)
	}
	if rlErr.Remaining != "0" {
		t.Fatalf("expected remaining=0, got %s", rlErr.Remaining)
	}
}

func TestFetchGamesRespectsMaxPagesCap(t *testing.T) {
	calls := 0
	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		body := `{
			"data": [
				{
					"id": 1,
					"date": "2024-01-01T00:00:00Z",
					"status": "Final",
					"home_team": { "id": 1, "full_name": "Home" },
					"visitor_team": { "id": 2, "full_name": "Away" },
					"home_team_score": 10,
					"visitor_team_score": 5,
					"season": 2023
				}
			],
			"meta": { "total_pages": 10 }
		}`
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
		}, nil
	})

	client := NewClient(Config{
		BaseURL:    "http://example.com",
		HTTPClient: &http.Client{Transport: rt},
		MaxPages:   1,
	})

	games, err := client.FetchGames(context.Background(), "", "")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(games) != 1 {
		t.Fatalf("expected 1 game, got %d", len(games))
	}
	if calls != 1 {
		t.Fatalf("expected to stop after max pages, got %d calls", calls)
	}
}

func TestNewClientSetsDefaultHTTPClient(t *testing.T) {
	c := NewClient(Config{})
	httpClient, ok := c.httpClient.(*http.Client)
	if !ok {
		t.Fatalf("expected default http client")
	}
	if httpClient.Timeout == 0 {
		t.Fatalf("expected timeout to be set on default http client")
	}
}

type roundTripperFunc func(req *http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		raw      string
		expected time.Duration
		validate func(time.Duration) bool
	}{
		{
			name:     "seconds",
			raw:      "15",
			expected: 15 * time.Second,
		},
		{
			name: "http_date",
			raw:  now.Add(90 * time.Second).UTC().Format(http.TimeFormat),
			validate: func(d time.Duration) bool {
				return d >= 80*time.Second && d <= 95*time.Second
			},
		},
		{
			name:     "empty",
			raw:      "",
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseRetryAfter(tt.raw, now)
			if tt.validate != nil {
				if !tt.validate(got) {
					t.Fatalf("validation failed for %s: %s", tt.name, got)
				}
				return
			}
			if got != tt.expected {
				t.Fatalf("expected %s, got %s", tt.expected, got)
			}
		})
	}
}

func TestRateLimitDetails(t *testing.T) {
	now := time.Unix(0, 0)
	body := []byte("slow down")
	tests := []struct {
		name        string
		status      int
		headers     http.Header
		expectedRL  bool
		expectedRA  time.Duration
		expectedRem string
	}{
		{
			name:        "429 with retry after",
			status:      http.StatusTooManyRequests,
			headers:     http.Header{"Retry-After": []string{"5"}, "X-Rate-Limit-Remaining": []string{"0"}},
			expectedRL:  true,
			expectedRA:  5 * time.Second,
			expectedRem: "0",
		},
		{
			name:        "503 treated as rate limited",
			status:      http.StatusServiceUnavailable,
			headers:     http.Header{},
			expectedRL:  true,
			expectedRA:  0,
			expectedRem: "",
		},
		{
			name:        "non rate limit status",
			status:      http.StatusBadGateway,
			headers:     http.Header{},
			expectedRL:  false,
			expectedRA:  0,
			expectedRem: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &http.Response{
				StatusCode: tt.status,
				Header:     tt.headers,
			}
			retryAfter, remaining, rateLimited, msg := rateLimitDetails(resp, body, now)

			if rateLimited != tt.expectedRL {
				t.Fatalf("expected rateLimited=%v, got %v", tt.expectedRL, rateLimited)
			}
			if retryAfter != tt.expectedRA {
				t.Fatalf("expected retryAfter %s, got %s", tt.expectedRA, retryAfter)
			}
			if remaining != tt.expectedRem {
				t.Fatalf("expected remaining %s, got %s", tt.expectedRem, remaining)
			}
			if msg == "" {
				t.Fatalf("expected message to be populated")
			}
		})
	}
}

func TestResolveDatePrefersParam(t *testing.T) {
	c := NewClient(Config{})
	got := c.resolveDate("2024-02-01", time.UTC)
	if got != "2024-02-01" {
		t.Fatalf("expected provided date, got %s", got)
	}
}
