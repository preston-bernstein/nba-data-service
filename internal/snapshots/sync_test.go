package snapshots

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"log/slog"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
)

func simpleSnapshot(date string) domaingames.TodayResponse {
	return domaingames.TodayResponse{
		Date: date,
		Games: []domaingames.Game{
			{ID: date},
		},
	}
}

func writeSnapshot(t *testing.T, w *Writer, date string, snap domaingames.TodayResponse) {
	t.Helper()
	if w == nil {
		t.Fatalf("writer is nil for date %s", date)
	}
	if err := w.WriteGamesSnapshot(date, snap); err != nil {
		t.Fatalf("failed to write snapshot %s: %v", date, err)
	}
}

func writeSimpleSnapshot(t *testing.T, w *Writer, date string) {
	t.Helper()
	writeSnapshot(t, w, date, simpleSnapshot(date))
}

func requireSnapshotExists(t *testing.T, w *Writer, date string) {
	t.Helper()
	if w == nil {
		t.Fatalf("writer is nil when asserting snapshot for %s", date)
	}
	path := filepath.Join(w.BasePath(), "games", date+".json")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected snapshot for %s to be written: %v", date, err)
	}
}

func assertDatesEqual(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("dates length mismatch: got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("dates mismatch at %d: got %v, want %v", i, got, want)
		}
	}
}

type recordingProvider struct {
	dates []string
}

func (p *recordingProvider) FetchGames(ctx context.Context, date string, _ string) ([]domaingames.Game, error) {
	p.dates = append(p.dates, date)
	return []domaingames.Game{{ID: date}}, nil
}

func TestSyncerBackfillsPastAndFuture(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer := NewWriter(t.TempDir(), 5000)
	now := time.Date(2024, 1, 10, 12, 0, 0, 0, time.UTC)
	provider := &recordingProvider{}
	cfg := SyncConfig{
		Enabled:    true,
		Days:       3,
		FutureDays: 2,
		Interval:   time.Nanosecond,
	}

	// Backfill wipes all snapshots first, then fetches full window (no pre-seeded skip).
	syncer := NewSyncer(provider, writer, cfg, nil, nil)
	syncer.now = func() time.Time { return now }

	syncer.Run(ctx)
	cancel()

	// Today, yesterday, 2 days ago, then future 2024-01-11 and 2024-01-12 (all fetched after wipe).
	expected := []string{"2024-01-10", "2024-01-09", "2024-01-08", "2024-01-11", "2024-01-12"}
	assertDatesEqual(t, provider.dates, expected)
	for _, date := range expected {
		requireSnapshotExists(t, writer, date)
	}
}

func TestSyncerSkipsWhenDisabledOrNil(t *testing.T) {
	s := NewSyncer(nil, nil, SyncConfig{Enabled: false}, nil, nil)
	s.Run(context.Background())

	s = NewSyncer(nil, nil, SyncConfig{Enabled: true}, nil, nil)
	s.Run(context.Background())
}

func TestSleepRespectsContext(t *testing.T) {
	s := NewSyncer(nil, nil, SyncConfig{Enabled: true}, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	s.sleep(ctx, time.Second)
	if time.Since(start) > 50*time.Millisecond {
		t.Fatalf("expected sleep to return quickly when context canceled")
	}
}

func TestHasSnapshotNilWriter(t *testing.T) {
	s := NewSyncer(nil, nil, SyncConfig{}, nil, nil)
	if s.hasSnapshot("2024-01-01") {
		t.Fatalf("expected hasSnapshot to be false with nil writer")
	}
}

func TestBuildDatesSkipsExistingSnapshots(t *testing.T) {
	w := NewWriter(t.TempDir(), 10000)
	writeSimpleSnapshot(t, w, "2024-01-03") // past (2 days ago; still always refreshed)
	writeSimpleSnapshot(t, w, "2024-01-06") // future

	s := NewSyncer(nil, w, SyncConfig{Enabled: true, Days: 5, FutureDays: 2}, nil, nil)
	now := time.Date(2024, 1, 5, 10, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	dates := s.buildDates(s.now())

	// Today, yesterday, and 2 days ago are always refreshed (so 2024-01-05, 2024-01-04, 2024-01-03).
	want := map[string]bool{
		"2024-01-05": true, // today
		"2024-01-04": true, // yesterday
		"2024-01-03": true, // 2 days ago (always refresh for final scores)
	}
	for _, d := range dates {
		if want[d] {
			delete(want, d)
		}
		if d == "2024-01-06" {
			t.Fatalf("expected existing future snapshot to be skipped, got %s", d)
		}
	}
	if len(want) != 0 {
		t.Fatalf("expected today, yesterday, and 2 days ago to be present, missing %v", want)
	}
}

func TestDailyUsesTicker(t *testing.T) {
	writer := NewWriter(t.TempDir(), 5)
	prov := &recordingProvider{}
	cfg := SyncConfig{
		Enabled:      true,
		Days:         2,
		FutureDays:   0,
		Interval:     time.Nanosecond,
		DailyHourUTC: time.Now().UTC().Hour(),
	}
	s := NewSyncer(prov, writer, cfg, nil, nil)
	s.now = func() time.Time { return time.Now().UTC() }
	s.newTicker = func(d time.Duration) *time.Ticker {
		return time.NewTicker(time.Millisecond)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.daily(ctx)
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()
	<-done

	if len(prov.dates) == 0 {
		t.Fatalf("expected daily loop to trigger sync at least once, got dates=%v", prov.dates)
	}
}

func TestDailyReturnsOnCancel(t *testing.T) {
	s := NewSyncer(nil, NewWriter(t.TempDir(), 1), SyncConfig{Enabled: true}, nil, nil)
	s.newTicker = func(d time.Duration) *time.Ticker { return time.NewTicker(time.Hour) }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.daily(ctx) // should exit immediately without blocking
}

type errProvider struct{ err error }

func (p errProvider) FetchGames(ctx context.Context, date string, tz string) ([]domaingames.Game, error) {
	return nil, p.err
}

type emptyProvider struct{}

func (emptyProvider) FetchGames(ctx context.Context, date string, tz string) ([]domaingames.Game, error) {
	return []domaingames.Game{}, nil
}

type goodProvider struct{ games []domaingames.Game }

func (p goodProvider) FetchGames(ctx context.Context, date string, tz string) ([]domaingames.Game, error) {
	return p.games, nil
}

func TestSyncerNormalizesConfig(t *testing.T) {
	s := NewSyncer(nil, nil, SyncConfig{
		Days:         0,
		FutureDays:   -1,
		Interval:     0,
		DailyHourUTC: -5,
	}, nil, nil)

	if s.cfg.Days != 7 {
		t.Fatalf("expected default days 7, got %d", s.cfg.Days)
	}
	if s.cfg.FutureDays != 0 {
		t.Fatalf("expected future days clamped to 0, got %d", s.cfg.FutureDays)
	}
	if s.cfg.Interval <= 0 {
		t.Fatalf("expected interval defaulted, got %s", s.cfg.Interval)
	}
	if s.cfg.DailyHourUTC != 2 {
		t.Fatalf("expected daily hour defaulted to 2, got %d", s.cfg.DailyHourUTC)
	}
}

func TestFetchAndWriteHandlesErrorsAndSuccess(t *testing.T) {
	dir := t.TempDir()
	logger := testLogger()

	// Provider error -> logWarn path, no panic.
	s := NewSyncer(errProvider{err: providers.ErrProviderUnavailable}, NewWriter(dir, 7), SyncConfig{Enabled: true}, logger, nil)
	s.fetchAndWrite(context.Background(), "2024-01-01")

	// Empty games -> logWarn path.
	s = NewSyncer(emptyProvider{}, NewWriter(dir, 7), SyncConfig{Enabled: true}, logger, nil)
	s.fetchAndWrite(context.Background(), "2024-01-02")

	// Writer failure (basePath is a file, so MkdirAll should fail).
	filePath := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(filePath, []byte("x"), 0o644); err != nil {
		t.Fatalf("failed to create placeholder file: %v", err)
	}
	s = NewSyncer(goodProvider{games: []domaingames.Game{{ID: "g1"}}}, &Writer{basePath: filePath, retentionDays: 1}, SyncConfig{Enabled: true}, logger, nil)
	s.fetchAndWrite(context.Background(), "2024-01-03")

	// Successful write path (large retention to avoid pruning).
	writer := NewWriter(t.TempDir(), 10000)
	s = NewSyncer(goodProvider{games: []domaingames.Game{{ID: "g2"}}}, writer, SyncConfig{Enabled: true}, logger, nil)
	s.fetchAndWrite(context.Background(), "2024-01-04")
	requireSnapshotExists(t, writer, "2024-01-04")
}

func TestRunSkipsWhenDisabled(t *testing.T) {
	prov := goodProvider{games: []domaingames.Game{{ID: "g1"}}}
	writer := NewWriter(t.TempDir(), 7)
	s := NewSyncer(prov, writer, SyncConfig{Enabled: false}, testLogger(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run(ctx) // should return immediately without panic
}

func TestBackfillRespectsContextCancel(t *testing.T) {
	prov := goodProvider{games: []domaingames.Game{{ID: "g1"}}}
	writer := NewWriter(t.TempDir(), 7)
	s := NewSyncer(prov, writer, SyncConfig{Enabled: true, Interval: time.Second}, testLogger(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.backfill(ctx, time.Now().UTC()) // should exit quickly without writing
}

func TestRunLogsInitialBackfillComplete(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	writer := NewWriter(t.TempDir(), 7)
	prov := goodProvider{games: []domaingames.Game{{ID: "g1"}}}
	s := NewSyncer(prov, writer, SyncConfig{Enabled: true, Days: 1, Interval: time.Nanosecond}, logger, nil)
	s.now = func() time.Time { return time.Date(2024, 1, 5, 10, 0, 0, 0, time.UTC) }

	ctx, cancel := context.WithCancel(context.Background())
	s.Run(ctx)
	cancel()

	if !strings.Contains(buf.String(), "snapshot sync initial backfill complete") {
		t.Fatalf("expected backfill completion log, got %s", buf.String())
	}
}

func TestBackfillLogsWhenSnapshotWipeFails(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "games"), []byte("x"), 0o644); err != nil {
		t.Fatalf("failed to create games file: %v", err)
	}
	writer := NewWriter(dir, 7)
	prov := goodProvider{games: []domaingames.Game{{ID: "g1"}}}
	s := NewSyncer(prov, writer, SyncConfig{Enabled: true, Days: 1, Interval: time.Nanosecond}, logger, nil)

	s.backfill(context.Background(), time.Date(2024, 1, 5, 10, 0, 0, 0, time.UTC))

	if !strings.Contains(buf.String(), "snapshot wipe failed") {
		t.Fatalf("expected snapshot wipe failed log, got %s", buf.String())
	}
}

// testLogger returns a no-op slog logger.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}
