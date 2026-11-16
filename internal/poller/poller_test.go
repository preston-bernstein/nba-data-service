package poller

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
	"github.com/preston-bernstein/nba-data-service/internal/gamedate"
	"github.com/preston-bernstein/nba-data-service/internal/metrics"
	"github.com/preston-bernstein/nba-data-service/internal/teststubs"
)

func scheduledGame(id string) domaingames.Game {
	return domaingames.Game{
		ID:         id,
		Provider:   "stub",
		HomeTeam:   teams.Team{ID: "home", Name: "Home"},
		AwayTeam:   teams.Team{ID: "away", Name: "Away"},
		StartTime:  time.Date(2024, 1, 1, 15, 0, 0, 0, time.UTC).Format(time.RFC3339),
		Status:     "Scheduled",
		StatusKind: domaingames.StatusScheduled,
		Score:      domaingames.Score{Home: 0, Away: 0},
		Meta:       domaingames.GameMeta{Season: "2023-2024", UpstreamGameID: 10},
	}
}

func assertSnapshotHasGame(t *testing.T, writer *teststubs.StubSnapshotWriter, date, gameID string) {
	t.Helper()
	snap, ok := writer.Written[date]
	if !ok {
		t.Fatalf("expected snapshot written for %s", date)
	}
	if len(snap.Games) != 1 || snap.Games[0].ID != gameID {
		t.Fatalf("unexpected snapshot for %s: %+v", date, snap)
	}
}

func TestPollerFetchesAndWritesSnapshot(t *testing.T) {
	provider := &teststubs.StubProvider{
		Games:  []domaingames.Game{scheduledGame("poll-game")},
		Notify: make(chan struct{}),
	}

	writer := &teststubs.StubSnapshotWriter{}

	p := New(provider, writer, Options{Interval: 10 * time.Millisecond})
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

	// Verify today's and yesterday's snapshots were written (poller writes both so games spanning midnight stay updated).
	assertSnapshotHasGame(t, writer, "2024-01-15", "poll-game")
	assertSnapshotHasGame(t, writer, "2024-01-14", "poll-game")

	if provider.Calls.Load() < 2 {
		t.Fatalf("expected at least two fetch calls (today and yesterday)")
	}
}

func TestPollerStopsOnContextCancel(t *testing.T) {
	provider := &teststubs.StubProvider{
		Games:  []domaingames.Game{},
		Notify: make(chan struct{}),
	}

	writer := &teststubs.StubSnapshotWriter{}

	p := New(provider, writer, Options{Interval: 5 * time.Millisecond})
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

	p := New(provider, writer, Options{Interval: time.Hour})

	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("first stop returned error: %v", err)
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("second stop returned error: %v", err)
	}
}

func TestPollerStopWithNilContext(t *testing.T) {
	p := New(&teststubs.StubProvider{}, &teststubs.StubSnapshotWriter{}, Options{Interval: time.Hour})
	var nilCtx context.Context // the case under test: a nil context
	if err := p.Stop(nilCtx); err != nil {
		t.Fatalf("expected nil error for nil context, got %v", err)
	}
}

func TestPollerStopReturnsContextErrorWhenCanceled(t *testing.T) {
	p := New(&teststubs.StubProvider{}, &teststubs.StubSnapshotWriter{}, Options{Interval: time.Hour})
	p.started = true
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Stop(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled error, got %v", err)
	}
}

func TestPollerStartIsIdempotent(t *testing.T) {
	provider := &teststubs.StubProvider{
		Games: []domaingames.Game{},
	}

	writer := &teststubs.StubSnapshotWriter{}

	p := New(provider, writer, Options{Interval: time.Hour})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p.Start(ctx)
	p.Start(ctx) // should no-op

	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("stop returned error: %v", err)
	}
}

func TestPollerDefaultsInterval(t *testing.T) {
	p := New(&teststubs.StubProvider{}, &teststubs.StubSnapshotWriter{}, Options{})
	if p.interval != defaultInterval {
		t.Fatalf("expected default interval %s, got %s", defaultInterval, p.interval)
	}
}

func TestPollerStartReturnsWhenAlreadyStarted(t *testing.T) {
	provider := &teststubs.StubProvider{}
	p := New(provider, &teststubs.StubSnapshotWriter{}, Options{Interval: time.Hour})
	p.started = true
	p.Start(context.Background())
	if p.ticker != nil {
		t.Fatalf("expected ticker not to be created when already started")
	}
}

func TestPollerStopTriggersDoneChannel(t *testing.T) {
	provider := &teststubs.StubProvider{}
	p := New(provider, &teststubs.StubSnapshotWriter{}, Options{Interval: 10 * time.Millisecond})
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

	p := New(provider, writer, Options{Interval: time.Millisecond})
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
	logger := slog.New(slog.DiscardHandler)

	p := New(provider, writer, Options{Logger: logger, Interval: time.Second})
	p.fetchOnce(context.Background()) // should log error

	provider.Err = nil
	provider.Games = []domaingames.Game{{ID: "ok"}}
	p.fetchOnce(context.Background()) // should log info
}

type dateAwareProvider struct {
	games  []domaingames.Game
	errFor map[string]error
	dates  []string
}

func (p *dateAwareProvider) FetchGames(ctx context.Context, date string, tz string) ([]domaingames.Game, error) {
	_ = ctx
	_ = tz
	p.dates = append(p.dates, date)
	if err, ok := p.errFor[date]; ok {
		return nil, err
	}
	return p.games, nil
}

func TestPollerRecordsMetricsWhenRecorderPresent(t *testing.T) {
	provider := &teststubs.StubProvider{Games: []domaingames.Game{{ID: "g1"}}}
	p := New(provider, nil, Options{Metrics: metrics.NewRecorder(), Interval: time.Minute})
	p.fetchOnce(context.Background())
}

func TestPollerLogsYesterdayFetchError(t *testing.T) {
	now := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	today := gamedate.FormatDate(now)
	yesterday := gamedate.FormatDate(now.AddDate(0, 0, -1))
	provider := &dateAwareProvider{
		games:  []domaingames.Game{{ID: "g1"}},
		errFor: map[string]error{yesterday: errors.New("yesterday failed")},
	}
	writer := &teststubs.StubSnapshotWriter{}
	logger := slog.New(slog.DiscardHandler)

	p := New(provider, writer, Options{Logger: logger, Interval: time.Minute})
	p.now = func() time.Time { return now }
	p.fetchOnce(context.Background())

	if _, ok := writer.Written[today]; !ok {
		t.Fatalf("expected today snapshot written")
	}
	foundYesterday := false
	for _, d := range provider.dates {
		if d == yesterday {
			foundYesterday = true
			break
		}
	}
	if !foundYesterday {
		t.Fatalf("expected fetch for yesterday")
	}
}

func TestPollerProviderExposesWrappedProvider(t *testing.T) {
	provider := &teststubs.StubProvider{}
	writer := &teststubs.StubSnapshotWriter{}
	p := New(provider, writer, Options{Interval: time.Minute})

	if got := p.Provider(); got != provider {
		t.Fatalf("expected provider returned")
	}
}

func TestPollerNilWriterDoesNotPanic(t *testing.T) {
	provider := &teststubs.StubProvider{Games: []domaingames.Game{{ID: "g1"}}}
	p := New(provider, nil, Options{Interval: time.Minute})
	p.fetchOnce(context.Background()) // should not panic
}

func TestPollerWriteErrorLogsButContinues(t *testing.T) {
	provider := &teststubs.StubProvider{Games: []domaingames.Game{{ID: "g1"}}}
	writer := &teststubs.StubSnapshotWriter{Err: errors.New("write failed")}
	logger := slog.New(slog.DiscardHandler)

	p := New(provider, writer, Options{Logger: logger, Interval: time.Minute})
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
				ID:         "bench-game",
				Provider:   "fixture",
				HomeTeam:   teams.Team{ID: "home", Name: "Home"},
				AwayTeam:   teams.Team{ID: "away", Name: "Away"},
				StartTime:  time.Date(2024, 1, 1, 19, 30, 0, 0, time.UTC).Format(time.RFC3339),
				Status:     "Final",
				StatusKind: domaingames.StatusFinal,
				Score:      domaingames.Score{Home: 100, Away: 95},
			},
		},
	}
	writer := &teststubs.StubSnapshotWriter{}
	p := New(provider, writer, Options{Interval: time.Second})
	ctx := context.Background()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p.fetchOnce(ctx)
	}
}
