package server

import (
	"log/slog"

	"github.com/preston-bernstein/nba-data-service/internal/config"
	"github.com/preston-bernstein/nba-data-service/internal/events"
)

// buildPublisher returns a Kafka publisher when KAFKA_BROKERS is set, else a no-op.
// If the client cannot be created it logs the error and falls back to the no-op,
// so a bad Kafka setting never stops the service.
func buildPublisher(cfg config.Config, logger *slog.Logger) events.Publisher {
	if len(cfg.Kafka.Brokers) == 0 {
		return events.NoopPublisher{}
	}
	pub, err := events.NewKafkaPublisher(cfg.Kafka.Brokers, cfg.Kafka.Topic)
	if err != nil {
		if logger != nil {
			logger.Error("kafka publisher disabled", "error", err)
		}
		return events.NoopPublisher{}
	}
	if logger != nil {
		logger.Info("kafka publisher enabled", "topic", cfg.Kafka.Topic)
	}
	return pub
}
