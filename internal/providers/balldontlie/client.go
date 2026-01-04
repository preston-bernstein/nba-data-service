package balldontlie

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain/players"
	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
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

	games, err := fetchPaged(ctx, c.maxPages, c.now, c.httpClient, buildReq, decode)
	if err != nil {
		return nil, err
	}
	return dedupe(games, func(g domaingames.Game) string { return g.ID }), nil
}

// FetchTeams retrieves teams from balldontlie.
func (c *Client) FetchTeams(ctx context.Context) ([]teams.Team, error) {
	buildReq := func(page int) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/teams", nil)
		if err != nil {
			return nil, err
		}
		q := req.URL.Query()
		q.Set("per_page", strconv.Itoa(defaultPerPage))
		q.Set("page", strconv.Itoa(page))
		req.URL.RawQuery = q.Encode()
		if c.apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+c.apiKey)
		}
		return req, nil
	}
	decode := func(dec *json.Decoder) ([]teams.Team, int, error) {
		var payload teamsResponse
		if err := dec.Decode(&payload); err != nil {
			return nil, 0, err
		}
		mapped := make([]teams.Team, 0, len(payload.Data))
		for _, t := range payload.Data {
			mapped = append(mapped, mapTeam(t))
		}
		return mapped, payload.Meta.TotalPages, nil
	}
	items, err := fetchPaged(ctx, c.maxPages, c.now, c.httpClient, buildReq, decode)
	if err != nil {
		return nil, err
	}
	return dedupe(items, func(t teams.Team) string { return t.ID }), nil
}

// FetchPlayers retrieves players from balldontlie.
func (c *Client) FetchPlayers(ctx context.Context) ([]players.Player, error) {
	buildReq := func(page int) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/players", nil)
		if err != nil {
			return nil, err
		}
		q := req.URL.Query()
		q.Set("per_page", strconv.Itoa(defaultPerPage))
		q.Set("page", strconv.Itoa(page))
		req.URL.RawQuery = q.Encode()
		if c.apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+c.apiKey)
		}
		return req, nil
	}
	decode := func(dec *json.Decoder) ([]players.Player, int, error) {
		var payload playersResponse
		if err := dec.Decode(&payload); err != nil {
			return nil, 0, err
		}
		mapped := make([]players.Player, 0, len(payload.Data))
		for _, pResp := range payload.Data {
			mapped = append(mapped, mapPlayer(pResp))
		}
		return mapped, payload.Meta.TotalPages, nil
	}
	items, err := fetchPaged(ctx, c.maxPages, c.now, c.httpClient, buildReq, decode)
	if err != nil {
		return nil, err
	}
	return dedupe(items, func(p players.Player) string { return p.ID }), nil
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

// fetchPaged centralizes pagination and error handling for list endpoints.
func fetchPaged[T any](
	ctx context.Context,
	maxPages int,
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
