package config

import "time"

const (
	envBdlBaseURL   = "BALLDONTLIE_BASE_URL"
	envBdlAPIKey    = "BALLDONTLIE_API_KEY"
	envBdlTimezone  = "BALLDONTLIE_TIMEZONE"
	envBdlMaxPages  = "BALLDONTLIE_MAX_PAGES"
	envBdlPageDelay = "BALLDONTLIE_PAGE_DELAY"

	defaultBdlBaseURL  = "https://api.balldontlie.io/v1"
	defaultBdlTimezone = "America/New_York"
	defaultBdlMaxPages = 5
)

// BalldontlieConfig controls how we talk to the balldontlie API.
type BalldontlieConfig struct {
	BaseURL   string
	APIKey    string
	Timezone  string
	MaxPages  int
	PageDelay time.Duration
}

func loadBalldontlie() BalldontlieConfig {
	return BalldontlieConfig{
		BaseURL:   envOrDefault(envBdlBaseURL, defaultBdlBaseURL),
		APIKey:    envOrDefault(envBdlAPIKey, ""),
		Timezone:  envOrDefault(envBdlTimezone, defaultBdlTimezone),
		MaxPages:  intEnvOrDefault(envBdlMaxPages, defaultBdlMaxPages),
		PageDelay: durationEnvOrDefault(envBdlPageDelay, 0),
	}
}

// Ensure Duration alias is used to avoid unused import of time in constants.
var _ = time.Second
