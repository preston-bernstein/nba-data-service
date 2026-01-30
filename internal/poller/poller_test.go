package poller

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
	"github.com/preston-bernstein/nba-data-service/internal/teststubs"
)

func TestPollerFetchesAndWritesSnapshot(t *testing.T) {
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

	provider := &teststubs.StubProvider{
		Games:  []domaingames.Game{g},
		Notify: make(chan struct{}),
	}

	writer := &teststubs.StubSnapshotWriter{}

	p := New(provider, writer, nil, nil, 10*time.Millisecond)
	// Fix the time for deterministic date.
	p.now = func() time.Time { return time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC) }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p.Start(ctx)

	select {
	case <-provider.Notify:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for initial fetch")
	}

	time.Sleep(30 * time.Millisecond) // allow at least one ticker fire

	cancel()
	_ = p.Stop(context.Background())

	// Verify snapshot was written.
	snap, ok := writer.Written["2024-01-15"]
	if !ok {
		t.Fatalf("expected snapshot written for 2024-01-15")
	}
	if len(snap.Games) != 1 || snap.Games[0].ID != "poll-game" {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}

	if provider.Calls.Load() < 1 {
		t.Fatalf("expected at least one fetch call")
	}
}

func TestPollerStopsOnContextCancel(t *testing.T) {
	provider := &teststubs.StubProvider{
		Games:  []domaingames.Game{},
		Notify: make(chan struct{}),
	}

	writer := &teststubs.StubSnapshotWriter{}

	p := New(provider, writer, nil, nil, 5*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())

	p.Start(ctx)

	// Wait for initial fetch.
	select {
	case <-provider.Notify:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for initial fetch")
	}

	cancel()
	_ = p.Stop(context.Background())

	callsAfterStop := provider.Calls.Load()
	time.Sleep(20 * time.Millisecond)
	if provider.Calls.Load() != callsAfterStop {
		t.Fatalf("expected no additional fetches after stop; before=%d after=%d", callsAfterStop, provider.Calls.Load())
	}
}

func TestPollerStopIsIdempotent(t *testing.T) {
	provider := &teststubs.StubProvider{
		Games: []domaingames.Game{},
	}

	writer := &teststubs.StubSnapshotWriter{}

	p := New(provider, writer, nil, nil, time.Hour)

	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("first stop returned error: %v", err)
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("second stop returned error: %v", err)
	}
}

func TestPollerStartIsIdempotent(t *testing.T) {
	provider := &teststubs.StubProvider{
		Games: []domaingames.Game{},
	}

	writer := &teststubs.StubSnapshotWriter{}

	p := New(provider, writer, nil, nil, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p.Start(ctx)
	p.Start(ctx) // should no-op

	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("stop returned error: %v", err)
	}
}

func TestPollerDefaultsInterval(t *testing.T) {
	p := New(&teststubs.StubProvider{}, &teststubs.StubSnapshotWriter{}, nil, nil, 0)
	if p.interval != defaultInterval {
		t.Fatalf("expected default interval %s, got %s", defaultInterval, p.interval)
	}
}

func TestPollerStartReturnsWhenAlreadyStarted(t *testing.T) {
	provider := &teststubs.StubProvider{}
	p := New(provider, &teststubs.StubSnapshotWriter{}, nil, nil, time.Hour)
	p.started = true
	p.Start(context.Background())
	if p.ticker != nil {
		t.Fatalf("expected ticker not to be created when already started")
	}
}

func TestPollerStopTriggersDoneChannel(t *testing.T) {
	provider := &teststubs.StubProvider{}
	p := New(provider, &teststubs.StubSnapshotWriter{}, nil, nil, 10*time.Millisecond)
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
	provider := &teststubs.StubProvider{
		Games: []domaingames.Game{},
		Err:   errors.New("boom"),
	}

	writer := &teststubs.StubSnapshotWriter{}

	p := New(provider, writer, nil, nil, time.Millisecond)
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

	provider.Err = nil
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
	provider := &teststubs.StubProvider{
		Err: errors.New("fail"),
	}
	writer := &teststubs.StubSnapshotWriter{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))

	p := New(provider, writer, logger, nil, time.Second)
	p.fetchOnce(context.Background()) // should log error

	provider.Err = nil
	provider.Games = []domaingames.Game{{ID: "ok"}}
	p.fetchOnce(context.Background()) // should log info
}

func TestPollerProviderExposesWrappedProvider(t *testing.T) {
	provider := &teststubs.StubProvider{}
	writer := &teststubs.StubSnapshotWriter{}
	p := New(provider, writer, nil, nil, time.Minute)

	if got := p.Provider(); got != provider {
		t.Fatalf("expected provider returned")
	}
}

func TestPollerNilWriterDoesNotPanic(t *testing.T) {
	provider := &teststubs.StubProvider{Games: []domaingames.Game{{ID: "g1"}}}
	p := New(provider, nil, nil, nil, time.Minute)
	p.fetchOnce(context.Background()) // should not panic
}

func TestPollerWriteErrorLogsButContinues(t *testing.T) {
	provider := &teststubs.StubProvider{Games: []domaingames.Game{{ID: "g1"}}}
	writer := &teststubs.StubSnapshotWriter{Err: errors.New("write failed")}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))

	p := New(provider, writer, logger, nil, time.Minute)
	p.fetchOnce(context.Background())

	// Should still record success even if write fails.
	if p.Status().ConsecutiveFailures != 0 {
		t.Fatalf("expected success despite write error")
	}
}

func BenchmarkPollerFetchOnce(b *testing.B) {
	provider := &teststubs.StubProvider{
		Games: []domaingames.Game{
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
	writer := &teststubs.StubSnapshotWriter{}
	p := New(provider, writer, nil, nil, time.Second)
	ctx := context.Background()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p.fetchOnce(ctx)
	}
}
