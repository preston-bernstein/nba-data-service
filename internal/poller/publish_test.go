package poller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/events"
	"github.com/preston-bernstein/nba-data-service/internal/teststubs"
)

type fakePublisher struct {
	mu     sync.Mutex
	events []events.Event
	err    error
	calls  int
}

func (f *fakePublisher) Publish(_ context.Context, ev events.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return f.err
	}
	f.events = append(f.events, ev)
	return nil
}
func (f *fakePublisher) Close() {}

func newPublishPoller(games []domaingames.Game, pub events.Publisher) (*Poller, *teststubs.StubProvider) {
	prov := &teststubs.StubProvider{Games: games}
	p := New(prov, nil, nil, nil, time.Hour, nil)
	p.now = func() time.Time { return time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC) }
	p.SetPublisher(pub)
	return p, prov
}

func game(id string, kind domaingames.GameStatusKind, home int) domaingames.Game {
	return domaingames.Game{ID: id, StatusKind: kind, Score: domaingames.Score{Home: home}}
}

func TestPollerPublishesOnlyOnChange(t *testing.T) {
	pub := &fakePublisher{}
	p, prov := newPublishPoller([]domaingames.Game{game("a", domaingames.StatusScheduled, 0)}, pub)
	ctx := context.Background()

	p.fetchOnce(ctx)
	if len(pub.events) != 1 || pub.events[0].ChangeKind != events.ChangeCreated {
		t.Fatalf("want one created event, got %+v", pub.events)
	}

	p.fetchOnce(ctx)
	if len(pub.events) != 1 {
		t.Fatalf("unchanged game must not publish, got %d", len(pub.events))
	}

	prov.Games = []domaingames.Game{game("a", domaingames.StatusInProgress, 0)}
	p.fetchOnce(ctx)
	prov.Games = []domaingames.Game{game("a", domaingames.StatusInProgress, 3)}
	p.fetchOnce(ctx)
	if len(pub.events) != 3 {
		t.Fatalf("want 3 events, got %d", len(pub.events))
	}
	if pub.events[1].ChangeKind != events.ChangeStatus || pub.events[2].ChangeKind != events.ChangeScore {
		t.Fatalf("wrong kinds: %s %s", pub.events[1].ChangeKind, pub.events[2].ChangeKind)
	}
	if pub.events[2].Key() != "a" {
		t.Fatalf("wrong key %q", pub.events[2].Key())
	}
}

func TestPublishFailureDoesNotBreakCycle(t *testing.T) {
	pub := &fakePublisher{err: errors.New("broker down")}
	p, _ := newPublishPoller([]domaingames.Game{game("a", domaingames.StatusScheduled, 0)}, pub)

	p.fetchOnce(context.Background())

	if !p.Status().IsReady() {
		t.Fatalf("cycle should still succeed: %+v", p.Status())
	}
	if pub.calls == 0 {
		t.Fatal("expected a publish attempt")
	}
}

func TestFailedPublishIsRetriedNextCycle(t *testing.T) {
	pub := &fakePublisher{err: errors.New("broker down")}
	p, _ := newPublishPoller([]domaingames.Game{game("a", domaingames.StatusScheduled, 0)}, pub)
	ctx := context.Background()

	p.fetchOnce(ctx)
	pub.err = nil
	p.fetchOnce(ctx)

	if len(pub.events) != 1 || pub.events[0].ChangeKind != events.ChangeCreated {
		t.Fatalf("want the missed event sent next cycle, got %+v", pub.events)
	}
}

func TestPollerWithoutPublisherStillWorks(t *testing.T) {
	p, _ := newPublishPoller([]domaingames.Game{game("a", domaingames.StatusScheduled, 0)}, nil)
	p.fetchOnce(context.Background())
	if !p.Status().IsReady() {
		t.Fatal("expected ready")
	}
}
