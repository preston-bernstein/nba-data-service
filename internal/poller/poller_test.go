package poller

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/preston-bernstein/nba-data-service/internal/app/games"
	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
	"github.com/preston-bernstein/nba-data-service/internal/store"
)

type stubProvider struct {
	games  []domaingames.Game
	err    error
	calls  atomic.Int32
	notify chan struct{}
}

func (s *stubProvider) FetchGames(ctx context.Context, date string, tz string) ([]domaingames.Game, error) {
	_ = ctx
	_ = date
	_ = tz
	if s.notify != nil {
		select {
		case <-s.notify:
		default:
			close(s.notify)
		}
	}
	s.calls.Add(1)
	return s.games, s.err
}

func TestPollerFetchesAndStoresGames(t *testing.T) {
	g := domaingames.Game{
		ID:        "poll-game",
		Provider:  "stub",
		HomeTeam:  teams.Team{ID: "home", Name: "Home"},
		AwayTeam:  teams.Team{ID: "away", Name: "Away"},
		StartTime: time.Date(2024, 1, 1, 15, 0, 0, 0, time.UTC).Format(time.RFC3339),
		Status:    domaingames.StatusScheduled,
		Score:     domaingames.Score{Home: 0, Away: 0},
		Meta:      domaingames.GameMeta{Season: "2023-2024", UpstreamGameID: 10},
	}

	provider := &stubProvider{
		games:  []domaingames.Game{g},
		notify: make(chan struct{}),
	}

	s := store.NewMemoryStore()
	svc := games.NewService(s)

	p := New(provider, svc, nil, nil, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p.Start(ctx)

	select {
	case <-provider.notify:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for initial fetch")
	}

	time.Sleep(30 * time.Millisecond) // allow at least one ticker fire

	cancel()
	_ = p.Stop(context.Background())

	if got := len(svc.Games()); got != 1 {
		t.Fatalf("expected 1 game stored, got %d", got)
	}

	if provider.calls.Load() < 1 {
		t.Fatalf("expected at least one fetch call")
	}
}

func TestPollerStopsOnContextCancel(t *testing.T) {
	provider := &stubProvider{
		games:  []domaingames.Game{},
		notify: make(chan struct{}),
	}

	s := store.NewMemoryStore()
	svc := games.NewService(s)

	p := New(provider, svc, nil, nil, 5*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())

	p.Start(ctx)

	// Wait for initial fetch.
	select {
	case <-provider.notify:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for initial fetch")
	}

	cancel()
	_ = p.Stop(context.Background())

	callsAfterStop := provider.calls.Load()
	time.Sleep(20 * time.Millisecond)
	if provider.calls.Load() != callsAfterStop {
		t.Fatalf("expected no additional fetches after stop; before=%d after=%d", callsAfterStop, provider.calls.Load())
	}
}

func TestPollerStopIsIdempotent(t *testing.T) {
	provider := &stubProvider{
		games: []domaingames.Game{},
	}

	s := store.NewMemoryStore()
	svc := games.NewService(s)

	p := New(provider, svc, nil, nil, time.Hour)

	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("first stop returned error: %v", err)
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("second stop returned error: %v", err)
	}
}

func TestPollerStartIsIdempotent(t *testing.T) {
	provider := &stubProvider{
		games: []domaingames.Game{},
	}

	s := store.NewMemoryStore()
	svc := games.NewService(s)

	p := New(provider, svc, nil, nil, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p.Start(ctx)
	p.Start(ctx) // should no-op

	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("stop returned error: %v", err)
	}
}

func TestPollerDefaultsInterval(t *testing.T) {
	p := New(&stubProvider{}, games.NewService(store.NewMemoryStore()), nil, nil, 0)
	if p.interval != defaultInterval {
		t.Fatalf("expected default interval %s, got %s", defaultInterval, p.interval)
	}
}

func TestPollerStartReturnsWhenAlreadyStarted(t *testing.T) {
	provider := &stubProvider{}
	p := New(provider, games.NewService(store.NewMemoryStore()), nil, nil, time.Hour)
	p.started = true
	p.Start(context.Background())
	if p.ticker != nil {
		t.Fatalf("expected ticker not to be created when already started")
	}
}

func TestPollerStopTriggersDoneChannel(t *testing.T) {
	provider := &stubProvider{}
	p := New(provider, games.NewService(store.NewMemoryStore()), nil, nil, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p.Start(ctx)
	time.Sleep(15 * time.Millisecond) // allow startup

	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("expected stop without error, got %v", err)
	}
	time.Sleep(10 * time.Millisecond) // allow goroutine to exit via done channel
}

func TestPollerStatusTracksFailuresAndSuccess(t *testing.T) {
	provider := &stubProvider{
		games: []domaingames.Game{},
		err:   errors.New("boom"),
	}

	s := store.NewMemoryStore()
	svc := games.NewService(s)

	p := New(provider, svc, nil, nil, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p.fetchOnce(ctx)
	status := p.Status()
	if status.ConsecutiveFailures != 1 {
		t.Fatalf("expected 1 failure, got %d", status.ConsecutiveFailures)
	}
	if status.LastError == "" {
		t.Fatalf("expected last error recorded")
	}
	if status.LastSuccess != (time.Time{}) {
		t.Fatalf("expected no success recorded yet")
	}
	if status.IsReady() {
		t.Fatalf("expected not ready after failure")
	}

	provider.err = nil
	p.fetchOnce(ctx)
	status = p.Status()
	if status.ConsecutiveFailures != 0 {
		t.Fatalf("expected failures reset, got %d", status.ConsecutiveFailures)
	}
	if status.LastSuccess.IsZero() {
		t.Fatalf("expected success timestamp")
	}
	if !status.IsReady() {
		t.Fatalf("expected ready after success")
	}
}

func TestPollerLogsOnErrorAndSuccess(t *testing.T) {
	provider := &stubProvider{
		err: errors.New("fail"),
	}
	s := store.NewMemoryStore()
	svc := games.NewService(s)
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))

	p := New(provider, svc, logger, nil, time.Second)
	p.fetchOnce(context.Background()) // should log error

	provider.err = nil
	provider.games = []domaingames.Game{{ID: "ok"}}
	p.fetchOnce(context.Background()) // should log info
}

func TestPollerProviderExposesWrappedProvider(t *testing.T) {
	provider := &stubProvider{}
	s := store.NewMemoryStore()
	svc := games.NewService(s)
	p := New(provider, svc, nil, nil, time.Minute)

	if got := p.Provider(); got != provider {
		t.Fatalf("expected provider returned")
	}
}

func BenchmarkPollerFetchOnce(b *testing.B) {
	provider := &stubProvider{
		games: []domaingames.Game{
			{
				ID:        "bench-game",
				Provider:  "fixture",
				HomeTeam:  teams.Team{ID: "home", Name: "Home"},
				AwayTeam:  teams.Team{ID: "away", Name: "Away"},
				StartTime: time.Date(2024, 1, 1, 19, 30, 0, 0, time.UTC).Format(time.RFC3339),
				Status:    domaingames.StatusFinal,
				Score:     domaingames.Score{Home: 100, Away: 95},
			},
		},
	}
	s := store.NewMemoryStore()
	svc := games.NewService(s)
	p := New(provider, svc, nil, nil, time.Second)
	ctx := context.Background()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p.fetchOnce(ctx)
	}
}
