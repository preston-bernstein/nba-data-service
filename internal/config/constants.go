package config

import "time"

const (
	envPort         = "PORT"
	envPollInterval = "POLL_INTERVAL"
	envProvider     = "PROVIDER"
	envMetricsPort  = "METRICS_PORT"
	envMetricsOn    = "METRICS_ENABLED"
	envOtelEndpoint = "OTEL_EXPORTER_OTLP_ENDPOINT"
	envOtelService  = "OTEL_SERVICE_NAME"
	envOtelInsecure = "OTEL_EXPORTER_OTLP_INSECURE"

	defaultPort         = "4000"
	defaultPollInterval = 30 * Duration(time.Second)
	defaultProvider     = "fixture"
	defaultMetricsPort  = "9090"
)
