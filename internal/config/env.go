package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Duration wraps time.Duration for clearer type usage in Config.
type Duration = time.Duration

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

func intEnvOrDefault(key string, defaultValue int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return defaultValue
	}
	val, err := strconv.Atoi(raw)
	if err != nil || val <= 0 {
		return defaultValue
	}
	return val
}

func boolEnvOrDefault(key string, defaultValue bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue
	}
	if raw == "1" || strings.EqualFold(raw, "true") || strings.EqualFold(raw, "yes") {
		return true
	}
	if raw == "0" || strings.EqualFold(raw, "false") || strings.EqualFold(raw, "no") {
		return false
	}
	return defaultValue
}
