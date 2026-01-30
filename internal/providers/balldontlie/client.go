package balldontlie

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
	"github.com/preston-bernstein/nba-data-service/internal/timeutil"
)

// Config controls how the balldontlie client reaches the upstream API.
type Config struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
	Timezone   string
	MaxPages   int
	PageDelay  time.Duration
}

// Client fetches games from the balldontlie API and maps them to domain models.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient httpDoer
	now        func() time.Time
	loc        *time.Location
	maxPages   int
	pageDelay  time.Duration
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
		pageDelay:  cfg.PageDelay,
	}
}

// FetchGames retrieves today's games from balldontlie.
func (c *Client) FetchGames(ctx context.Context, date string, tz string) ([]domaingames.Game, error) {
	loc := c.loc
	if tz != "" {
		if override := resolveLocation(tz); override != nil {
			loc = override
		}
	}

	buildReq := func(page int) (*http.Request, error) {
		return c.buildRequest(ctx, date, page, loc)
	}
	decode := func(dec *json.Decoder) ([]domaingames.Game, int, error) {
		var payload gamesResponse
		if err := dec.Decode(&payload); err != nil {
			return nil, 0, err
		}
		mapped := make([]domaingames.Game, 0, len(payload.Data))
		for _, g := range payload.Data {
			mapped = append(mapped, mapGame(g))
		}
		return mapped, payload.Meta.TotalPages, nil
	}

	games, err := fetchPaged(ctx, c.maxPages, c.pageDelay, c.now, c.httpClient, buildReq, decode)
	if err != nil {
		return nil, err
	}
	return dedupe(games, func(g domaingames.Game) string { return g.ID }), nil
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
		if _, err := timeutil.ParseDate(date); err == nil {
			return date
		}
	}
	return timeutil.FormatDate(c.now().In(loc))
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
	return errors.New(msg)
}

// fetchPaged centralizes pagination and error handling for list endpoints.
func fetchPaged[T any](
	ctx context.Context,
	maxPages int,
	pageDelay time.Duration,
	now func() time.Time,
	doer httpDoer,
	buildReq func(page int) (*http.Request, error),
	decode func(dec *json.Decoder) ([]T, int, error),
) ([]T, error) {
	page := 1
	var all []T

	for {
		req, err := buildReq(page)
		if err != nil {
			return nil, err
		}

		resp, err := doer.Do(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			resp.Body.Close()
			return nil, classifyErrorResponse(resp, body, now())
		}

		data, totalPages, err := decode(json.NewDecoder(resp.Body))
		resp.Body.Close()
		if err != nil {
			return nil, err
		}

		all = append(all, data...)

		if totalPages > 0 {
			if page >= totalPages || page >= maxPages {
				break
			}
		} else {
			if len(data) == 0 || len(data) < defaultPerPage || page >= maxPages {
				break
			}
		}
		if pageDelay > 0 {
			select {
			case <-ctx.Done():
				return all, ctx.Err()
			case <-time.After(pageDelay):
			}
		}
		page++
	}

	return all, nil
}

func dedupe[T any](items []T, idFn func(T) string) []T {
	unique := make(map[string]T, len(items))
	for _, item := range items {
		unique[idFn(item)] = item
	}
	ids := make([]string, 0, len(unique))
	for id := range unique {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := make([]T, 0, len(ids))
	for _, id := range ids {
		result = append(result, unique[id])
	}
	return result
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
