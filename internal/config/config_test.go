package config

import (
	"testing"
	"time"
)

type fieldCheck struct {
	name string
	got  any
	want any
}

func assertFields(t *testing.T, checks []fieldCheck) {
	t.Helper()
	for _, c := range checks {
		if c.got != c.want {
			t.Fatalf("%s: expected %v, got %v", c.name, c.want, c.got)
		}
	}
}

func setEnv(t *testing.T, env map[string]string) {
	t.Helper()
	for key, value := range env {
		t.Setenv(key, value)
	}
}

func TestLoadDefaults(t *testing.T) {
	setEnv(t, map[string]string{
		envPort: "", envPollInterval: "", envProvider: "",
		envBdlBaseURL: "", envBdlKeyVar: "", envBdlTimezone: "", envBdlMaxPages: "",
		envMetricsPort: "", envMetricsOn: "", envOtelEndpoint: "", envOtelService: "", envOtelInsecure: "",
		envSnapshotSync: "", envSnapshotDays: "", envSnapshotFutureDays: "", envSnapshotRate: "", envSnapshotHour: "",
	})

	cfg := Load()

	assertFields(t, []fieldCheck{
		{"port", cfg.Port, defaultPort},
		{"poll interval", cfg.PollInterval, defaultPollInterval},
		{"provider", cfg.Provider, defaultProvider},
		{"balldontlie base url", cfg.Balldontlie.BaseURL, defaultBdlBaseURL},
		{"balldontlie api key", cfg.Balldontlie.APIKey, ""},
		{"balldontlie timezone", cfg.Balldontlie.Timezone, defaultBdlTimezone},
		{"balldontlie max pages", cfg.Balldontlie.MaxPages, defaultBdlMaxPages},
		{"metrics enabled", cfg.Metrics.Enabled, true},
		{"metrics port", cfg.Metrics.Port, defaultMetricsPort},
		{"otlp endpoint", cfg.Metrics.OtlpEndpoint, ""},
		{"service name", cfg.Metrics.ServiceName, "nba-data-service"},
		{"otlp insecure", cfg.Metrics.OtlpInsecure, true},
		{"snapshot sync enabled", cfg.Snapshots.Enabled, true},
		{"snapshot days", cfg.Snapshots.Days, defaultSnapshotDays},
		{"snapshot future days", cfg.Snapshots.FutureDays, defaultSnapshotFutureDays},
		{"snapshot interval", cfg.Snapshots.Interval, defaultSnapshotInterval},
		{"snapshot daily hour", cfg.Snapshots.DailyHourUTC, defaultSnapshotDailyHour},
		{"retention days", cfg.Snapshots.RetentionDays, defaultSnapshotDays + 1},
	})
}

func TestLoadOverrides(t *testing.T) {
	setEnv(t, map[string]string{
		envPort: "5000", envPollInterval: "45s", envProvider: "balldontlie",
		envBdlBaseURL: "http://example.com/api", envBdlKeyVar: "secret-key", envBdlTimezone: "UTC", envBdlMaxPages: "2",
		envMetricsOn: "false", envMetricsPort: "9999", envOtelEndpoint: "http://otel-collector:4318",
		envOtelService: "custom-service", envOtelInsecure: "false",
		envSnapshotSync: "false", envSnapshotDays: "3", envSnapshotFutureDays: "4", envSnapshotRate: "1m", envSnapshotHour: "5",
	})

	cfg := Load()

	assertFields(t, []fieldCheck{
		{"port", cfg.Port, "5000"},
		{"poll interval", cfg.PollInterval, 45 * time.Second},
		{"provider", cfg.Provider, "balldontlie"},
		{"balldontlie base url", cfg.Balldontlie.BaseURL, "http://example.com/api"},
		{"balldontlie api key", cfg.Balldontlie.APIKey, "secret-key"},
		{"balldontlie timezone", cfg.Balldontlie.Timezone, "UTC"},
		{"balldontlie max pages", cfg.Balldontlie.MaxPages, 2},
		{"metrics enabled", cfg.Metrics.Enabled, false},
		{"metrics port", cfg.Metrics.Port, "9999"},
		{"otlp endpoint", cfg.Metrics.OtlpEndpoint, "http://otel-collector:4318"},
		{"service name", cfg.Metrics.ServiceName, "custom-service"},
		{"otlp insecure", cfg.Metrics.OtlpInsecure, false},
		{"snapshot sync enabled", cfg.Snapshots.Enabled, false},
		{"snapshot days", cfg.Snapshots.Days, 3},
		{"snapshot future days", cfg.Snapshots.FutureDays, 4},
		{"snapshot interval", cfg.Snapshots.Interval, time.Minute},
		{"snapshot daily hour", cfg.Snapshots.DailyHourUTC, 5},
		{"retention days", cfg.Snapshots.RetentionDays, 4},
	})
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

func TestLoadKafkaDefaults(t *testing.T) {
	t.Setenv(envKafkaBrokers, "")
	t.Setenv(envKafkaTopic, "")
	cfg := Load()
	if len(cfg.Kafka.Brokers) != 0 {
		t.Fatalf("expected no brokers, got %v", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.Topic != "game.updated" {
		t.Fatalf("unexpected topic %q", cfg.Kafka.Topic)
	}
}

func TestLoadKafkaBrokersList(t *testing.T) {
	t.Setenv(envKafkaBrokers, " a:9092, b:9092 ,,")
	t.Setenv(envKafkaTopic, "custom")
	cfg := Load()
	if len(cfg.Kafka.Brokers) != 2 || cfg.Kafka.Brokers[0] != "a:9092" || cfg.Kafka.Brokers[1] != "b:9092" {
		t.Fatalf("unexpected brokers %v", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.Topic != "custom" {
		t.Fatalf("unexpected topic %q", cfg.Kafka.Topic)
	}
}
