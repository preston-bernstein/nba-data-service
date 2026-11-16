package server

import (
	"context"
	"testing"

	"github.com/preston-bernstein/nba-data-service/internal/config"
	"github.com/preston-bernstein/nba-data-service/internal/events"
	"github.com/preston-bernstein/nba-data-service/internal/testkit"
)

func TestBuildPublisherDisabledWithoutBrokers(t *testing.T) {
	pub := buildPublisher(config.Config{}, nil)
	if _, ok := pub.(events.NoopPublisher); !ok {
		t.Fatalf("expected noop publisher, got %T", pub)
	}
}

type closeSpy struct{ closed bool }

func (c *closeSpy) Publish(context.Context, events.Event) error { return nil }
func (c *closeSpy) Close()                                      { c.closed = true }

func TestGracefulShutdownClosesPublisher(t *testing.T) {
	spy := &closeSpy{}
	srv := newServerWithDeps(config.Config{}, nil, &testkit.StubHTTPServer{}, &providerPoller{})
	srv.publisher = spy
	srv.gracefulShutdown(context.Background())
	if !spy.closed {
		t.Fatal("publisher not closed")
	}
}
