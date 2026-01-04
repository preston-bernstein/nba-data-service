package config

import "time"

const (
	envPort               = "PORT"
	envPollInterval       = "POLL_INTERVAL"
	envProvider           = "PROVIDER"
	envMetricsPort        = "METRICS_PORT"
	envMetricsOn          = "METRICS_ENABLED"
	envOtelEndpoint       = "OTEL_EXPORTER_OTLP_ENDPOINT"
	envOtelService        = "OTEL_SERVICE_NAME"
	envOtelInsecure       = "OTEL_EXPORTER_OTLP_INSECURE"
	envAdminToken         = "ADMIN_TOKEN"
	envSnapshotSync       = "SNAPSHOT_SYNC_ENABLED"
	envSnapshotDays       = "SNAPSHOT_SYNC_DAYS"
	envSnapshotFutureDays = "SNAPSHOT_FUTURE_DAYS"
	envSnapshotRate       = "SNAPSHOT_SYNC_INTERVAL"
	envSnapshotHour       = "SNAPSHOT_DAILY_HOUR"

	defaultPort = "4000"
	// Conservative default poll interval to respect upstream quotas (balldontlie: 5 req/min).
	defaultPollInterval       = 2 * Duration(time.Minute)
	defaultProvider           = "fixture"
	defaultMetricsPort        = "9090"
	defaultSnapshotSync       = true
	defaultSnapshotDays       = 7
	defaultSnapshotFutureDays = 7
	// Snapshot fetch cadence during backfill; spaced to stay under upstream quota and leave headroom.
	defaultSnapshotInterval = 90 * Duration(time.Second)
	// UTC hour to run daily snapshot prune/backfill (2 AM UTC by default).
	defaultSnapshotDailyHour = 2
)
