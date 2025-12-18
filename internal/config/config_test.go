package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv(envPort, "")
	t.Setenv(envPollInterval, "")
	t.Setenv(envProvider, "")
	t.Setenv(envBdlBaseURL, "")
	t.Setenv(envBdlAPIKey, "")
	t.Setenv(envBdlTimezone, "")
	t.Setenv(envBdlMaxPages, "")
	t.Setenv(envMetricsPort, "")
	t.Setenv(envMetricsOn, "")
	t.Setenv(envOtelEndpoint, "")
	t.Setenv(envOtelService, "")
	t.Setenv(envOtelInsecure, "")

	cfg := Load()

	if cfg.Port != defaultPort {
		t.Fatalf("expected default port %s, got %s", defaultPort, cfg.Port)
	}
	if cfg.PollInterval != defaultPollInterval {
		t.Fatalf("expected default poll interval %s, got %s", defaultPollInterval, cfg.PollInterval)
	}
	if cfg.Provider != defaultProvider {
		t.Fatalf("expected default provider %s, got %s", defaultProvider, cfg.Provider)
	}
	if cfg.Balldontlie.BaseURL != defaultBdlBaseURL {
		t.Fatalf("expected default balldontlie base url %s, got %s", defaultBdlBaseURL, cfg.Balldontlie.BaseURL)
	}
	if cfg.Balldontlie.APIKey != "" {
		t.Fatalf("expected empty balldontlie api key by default, got %s", cfg.Balldontlie.APIKey)
	}
	if cfg.Balldontlie.Timezone != defaultBdlTimezone {
		t.Fatalf("expected default balldontlie timezone %s, got %s", defaultBdlTimezone, cfg.Balldontlie.Timezone)
	}
	if cfg.Balldontlie.MaxPages != defaultBdlMaxPages {
		t.Fatalf("expected default balldontlie max pages %d, got %d", defaultBdlMaxPages, cfg.Balldontlie.MaxPages)
	}
	if !cfg.Metrics.Enabled {
		t.Fatalf("expected metrics enabled by default")
	}
	if cfg.Metrics.Port != defaultMetricsPort {
		t.Fatalf("expected default metrics port %s, got %s", defaultMetricsPort, cfg.Metrics.Port)
	}
	if cfg.Metrics.OtlpEndpoint != "" {
		t.Fatalf("expected empty otlp endpoint by default, got %s", cfg.Metrics.OtlpEndpoint)
	}
	if cfg.Metrics.ServiceName != "nba-games-service" {
		t.Fatalf("expected default service name nba-games-service, got %s", cfg.Metrics.ServiceName)
	}
	if !cfg.Metrics.OtlpInsecure {
		t.Fatalf("expected otlp insecure default true")
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv(envPort, "5000")
	t.Setenv(envPollInterval, "45s")
	t.Setenv(envProvider, "balldontlie")
	t.Setenv(envBdlBaseURL, "http://example.com/api")
	t.Setenv(envBdlAPIKey, "secret-key")
	t.Setenv(envBdlTimezone, "UTC")
	t.Setenv(envBdlMaxPages, "2")
	t.Setenv(envMetricsOn, "false")
	t.Setenv(envMetricsPort, "9999")
	t.Setenv(envOtelEndpoint, "http://otel-collector:4318")
	t.Setenv(envOtelService, "custom-service")
	t.Setenv(envOtelInsecure, "false")

	cfg := Load()

	if cfg.Port != "5000" {
		t.Fatalf("expected port 5000, got %s", cfg.Port)
	}
	if cfg.PollInterval != 45*time.Second {
		t.Fatalf("expected poll interval 45s, got %s", cfg.PollInterval)
	}
	if cfg.Provider != "balldontlie" {
		t.Fatalf("expected provider balldontlie, got %s", cfg.Provider)
	}
	if cfg.Balldontlie.BaseURL != "http://example.com/api" {
		t.Fatalf("expected balldontlie base url override, got %s", cfg.Balldontlie.BaseURL)
	}
	if cfg.Balldontlie.APIKey != "secret-key" {
		t.Fatalf("expected balldontlie api key override, got %s", cfg.Balldontlie.APIKey)
	}
	if cfg.Balldontlie.Timezone != "UTC" {
		t.Fatalf("expected balldontlie timezone override, got %s", cfg.Balldontlie.Timezone)
	}
	if cfg.Balldontlie.MaxPages != 2 {
		t.Fatalf("expected balldontlie max pages override, got %d", cfg.Balldontlie.MaxPages)
	}
	if cfg.Metrics.Enabled {
		t.Fatalf("expected metrics disabled via env override")
	}
	if cfg.Metrics.Port != "9999" {
		t.Fatalf("expected metrics port override, got %s", cfg.Metrics.Port)
	}
	if cfg.Metrics.OtlpEndpoint != "http://otel-collector:4318" {
		t.Fatalf("expected otlp endpoint override, got %s", cfg.Metrics.OtlpEndpoint)
	}
	if cfg.Metrics.ServiceName != "custom-service" {
		t.Fatalf("expected service name override, got %s", cfg.Metrics.ServiceName)
	}
	if cfg.Metrics.OtlpInsecure {
		t.Fatalf("expected otlp insecure false override")
	}
}

func TestLoadInvalidDurationFallsBack(t *testing.T) {
	t.Setenv(envPollInterval, "not-a-duration")

	cfg := Load()

	if cfg.PollInterval != defaultPollInterval {
		t.Fatalf("expected default poll interval on invalid value, got %s", cfg.PollInterval)
	}
}

func TestLoadNonPositiveDurationFallsBack(t *testing.T) {
	t.Setenv(envPollInterval, "0s")

	cfg := Load()

	if cfg.PollInterval != defaultPollInterval {
		t.Fatalf("expected default poll interval on non-positive value, got %s", cfg.PollInterval)
	}
}
