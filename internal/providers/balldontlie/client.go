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
	"github.com/preston-bernstein/nba-data-service/internal/gamedate"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
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
			return nil, 0, err //nolint:wrapcheck // error text is logged and shown to callers as is; wrapping changes it
		}
		mapped := make([]domaingames.Game, 0, len(payload.Data))
		for _, g := range payload.Data {
			mapped = append(mapped, mapGame(g))
		}
		return mapped, payload.Meta.TotalPages, nil
	}

	games, err := fetchPaged(ctx, pageSource[domaingames.Game]{
		maxPages:  c.maxPages,
		pageDelay: c.pageDelay,
		now:       c.now,
		doer:      c.httpClient,
		buildReq:  buildReq,
		decode:    decode,
	})
	if err != nil {
		return nil, err
	}
	return dedupe(games, func(g domaingames.Game) string { return g.ID }), nil
}

func (c *Client) buildRequest(ctx context.Context, date string, page int, loc *time.Location) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/games", nil)
	if err != nil {
		return nil, err //nolint:wrapcheck // error text is logged and shown to callers as is; wrapping changes it
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
		if _, err := gamedate.ParseDate(date); err == nil {
			return date
		}
	}
	return gamedate.FormatDate(c.now().In(loc))
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

// pageSource describes how to fetch and decode one page of a list endpoint.
type pageSource[T any] struct {
	maxPages  int
	pageDelay time.Duration
	now       func() time.Time
	doer      httpDoer
	buildReq  func(page int) (*http.Request, error)
	decode    func(dec *json.Decoder) ([]T, int, error)
}

// fetchPaged centralizes pagination and error handling for list endpoints.
func fetchPaged[T any](ctx context.Context, src pageSource[T]) ([]T, error) {
	var all []T

	for page := 1; ; page++ {
		data, totalPages, err := src.fetchPage(page)
		if err != nil {
			return nil, err
		}
		all = append(all, data...)

		if !src.hasMore(page, totalPages, len(data)) {
			return all, nil
		}
		if err := src.wait(ctx); err != nil {
			return all, err
		}
	}
}

func (s pageSource[T]) fetchPage(page int) ([]T, int, error) {
	req, err := s.buildReq(page)
	if err != nil {
		return nil, 0, err
	}

	resp, err := s.doer.Do(req)
	if err != nil {
		return nil, 0, err //nolint:wrapcheck // error text is logged and shown to callers as is; wrapping changes it
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, 0, classifyErrorResponse(resp, body, s.now())
	}

	return s.decode(json.NewDecoder(resp.Body))
}

// hasMore reports whether another page follows. Without a total page count, a short page is the last one.
func (s pageSource[T]) hasMore(page, totalPages, received int) bool {
	if page >= s.maxPages {
		return false
	}
	if totalPages > 0 {
		return page < totalPages
	}
	return received > 0 && received >= defaultPerPage
}

func (s pageSource[T]) wait(ctx context.Context) error {
	if s.pageDelay <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err() //nolint:wrapcheck // context error is returned as is; wrapping changes its message and identity
	case <-time.After(s.pageDelay):
		return nil
	}
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
