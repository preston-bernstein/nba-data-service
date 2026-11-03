package config

import "strings"

// KafkaConfig controls event publishing. No brokers means publishing is off.
type KafkaConfig struct {
	Brokers []string
	Topic   string
}

func loadKafka() KafkaConfig {
	var brokers []string
	for _, b := range strings.Split(envOrDefault(envKafkaBrokers, ""), ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	return KafkaConfig{
		Brokers: brokers,
		Topic:   envOrDefault(envKafkaTopic, defaultKafkaTopic),
	}
}
