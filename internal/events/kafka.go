package events

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaPublisher publishes events with franz-go. The record key is the game id.
type KafkaPublisher struct {
	client *kgo.Client
	topic  string
}

// NewKafkaPublisher creates a publisher. The client connects lazily,
// so this succeeds even when the broker is down.
func NewKafkaPublisher(brokers []string, topic string) (*KafkaPublisher, error) {
	if topic == "" {
		topic = DefaultTopic
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		// kafkajs cannot read snappy, the franz-go default.
		kgo.ProducerBatchCompression(kgo.GzipCompression()),
		kgo.RecordDeliveryTimeout(10*time.Second),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	return &KafkaPublisher{client: client, topic: topic}, nil
}

// Publish sends one event and waits for the broker to acknowledge it.
func (p *KafkaPublisher) Publish(ctx context.Context, ev Event) error {
	value, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	rec := &kgo.Record{Key: []byte(ev.Key()), Value: value}
	return p.client.ProduceSync(ctx, rec).FirstErr()
}

// Close flushes buffered records and closes the client.
func (p *KafkaPublisher) Close() {
	p.client.Close()
}
