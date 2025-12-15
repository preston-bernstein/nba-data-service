package config

import (
	"os"
	"time"
)

const (
	defaultPort         = "4000"
	defaultPollInterval = 30 * time.Second
	defaultProvider     = "fixture"
)

// Config holds runtime configuration for the server.
type Config struct {
	Port         string
	PollInterval time.Duration
	Provider     string
}

// Load reads configuration from environment variables with sensible defaults.
func Load() Config {
	return Config{
		Port:         envOrDefault("PORT", defaultPort),
		PollInterval: durationEnvOrDefault("POLL_INTERVAL", defaultPollInterval),
		Provider:     envOrDefault("PROVIDER", defaultProvider),
	}
}

func envOrDefault(key, defaultValue string) string {
	val := os.Getenv(key)
	if val != "" {
		return val
	}
	return defaultValue
}

func durationEnvOrDefault(key string, defaultValue time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return defaultValue
	}

	parsed, err := time.ParseDuration(raw)
	if err != nil || parsed <= 0 {
		return defaultValue
	}
	return parsed
}
