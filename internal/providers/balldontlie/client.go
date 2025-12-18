package balldontlie

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"nba-games-service/internal/domain"
	"nba-games-service/internal/providers"
)

// Config controls how the balldontlie client reaches the upstream API.
type Config struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
	Timezone   string
	MaxPages   int
}

// Client fetches games from the balldontlie API and maps them to domain models.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient httpDoer
	now        func() time.Time
	loc        *time.Location
	maxPages   int
}

// NewClient constructs a balldontlie client with the provided configuration.
func NewClient(cfg Config) *Client {
	return &Client{
		baseURL:    normalizeBaseURL(cfg.BaseURL),
		apiKey:     cfg.APIKey,
		httpClient: resolveHTTPClient(cfg.HTTPClient),
		now:        time.Now,
		loc:        resolveLocation(cfg.Timezone),
		maxPages:   resolveMaxPages(cfg.MaxPages),
	}
}

// FetchGames retrieves today's games from balldontlie.
func (c *Client) FetchGames(ctx context.Context, date string, tz string) ([]domain.Game, error) {
	loc := c.loc
	if tz != "" {
		if override := resolveLocation(tz); override != nil {
			loc = override
		}
	}

	page := 1
	allGames := make([]domain.Game, 0)

	for {
		req, err := c.buildRequest(ctx, date, page, loc)
		if err != nil {
			return nil, err
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			resp.Body.Close()
			return nil, classifyErrorResponse(resp, body, c.now())
		}

		var payload gamesResponse
		if decodeErr := json.NewDecoder(resp.Body).Decode(&payload); decodeErr != nil {
			resp.Body.Close()
			return nil, decodeErr
		}
		resp.Body.Close()

		for _, g := range payload.Data {
			allGames = append(allGames, mapGame(g))
		}

		totalPages := payload.Meta.TotalPages
		if totalPages > 0 {
			if page >= totalPages {
				break
			}
		} else {
			if len(payload.Data) == 0 || len(payload.Data) < defaultPerPage {
				break
			}
		}
		if page >= c.maxPages {
			break
		}
		page++
	}

	return allGames, nil
}

func (c *Client) buildRequest(ctx context.Context, date string, page int, loc *time.Location) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/games", nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Set("dates[]", c.resolveDate(date, loc))
	q.Set("per_page", strconv.Itoa(defaultPerPage))
	q.Set("page", strconv.Itoa(page))
	req.URL.RawQuery = q.Encode()

	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	return req, nil
}

func (c *Client) resolveDate(date string, loc *time.Location) string {
	if date != "" {
		if _, err := time.Parse("2006-01-02", date); err == nil {
			return date
		}
	}
	return c.now().In(loc).Format("2006-01-02")
}

func classifyErrorResponse(resp *http.Response, body []byte, now time.Time) error {
	retryAfter, remaining, rateLimited, msg := rateLimitDetails(resp, body, now)
	if rateLimited {
		return &providers.RateLimitError{
			Provider:   "balldontlie",
			StatusCode: resp.StatusCode,
			RetryAfter: retryAfter,
			Remaining:  remaining,
			Message:    msg,
		}
	}
	return fmt.Errorf(msg)
}

// parseRetryAfter interprets Retry-After header values as either seconds or HTTP dates.
func parseRetryAfter(raw string, now time.Time) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}

	if seconds, err := strconv.Atoi(raw); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}

	if ts, err := http.ParseTime(raw); err == nil {
		if ts.After(now) {
			return ts.Sub(now)
		}
	}

	return 0
}

func rateLimitDetails(resp *http.Response, body []byte, now time.Time) (time.Duration, string, bool, string) {
	msg := fmt.Sprintf("balldontlie: unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode != http.StatusServiceUnavailable {
		return 0, "", false, msg
	}

	retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"), now)
	remaining := resp.Header.Get("X-Rate-Limit-Remaining")

	return retryAfter, remaining, true, msg
}
