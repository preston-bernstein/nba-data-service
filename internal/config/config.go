package config

// Config holds runtime configuration for the server.
type Config struct {
	Port         string
	PollInterval Duration
	Provider     string
	Balldontlie  BalldontlieConfig
	Metrics      MetricsConfig
}

// Load reads configuration from environment variables with sensible defaults.
func Load() Config {
	return Config{
		Port:         envOrDefault(envPort, defaultPort),
		PollInterval: durationEnvOrDefault(envPollInterval, defaultPollInterval),
		Provider:     envOrDefault(envProvider, defaultProvider),
		Balldontlie:  loadBalldontlie(),
		Metrics:      loadMetrics(),
	}
}
