package events

import "context"

// Publisher sends events to a broker.
type Publisher interface {
	Publish(ctx context.Context, ev Event) error
	Close()
}

// NoopPublisher is used when Kafka is not configured.
type NoopPublisher struct{}

func (NoopPublisher) Publish(context.Context, Event) error { return nil }
func (NoopPublisher) Close()                               {}
