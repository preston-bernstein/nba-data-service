package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
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
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("PORT", "5000")
	t.Setenv("POLL_INTERVAL", "45s")
	t.Setenv("PROVIDER", "balldontlie")

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
}

func TestLoadInvalidDurationFallsBack(t *testing.T) {
	t.Setenv("POLL_INTERVAL", "not-a-duration")

	cfg := Load()

	if cfg.PollInterval != defaultPollInterval {
		t.Fatalf("expected default poll interval on invalid value, got %s", cfg.PollInterval)
	}
}

func TestLoadNonPositiveDurationFallsBack(t *testing.T) {
	t.Setenv("POLL_INTERVAL", "0s")

	cfg := Load()

	if cfg.PollInterval != defaultPollInterval {
		t.Fatalf("expected default poll interval on non-positive value, got %s", cfg.PollInterval)
	}
}
