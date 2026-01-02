package snapshots

import (
	"context"
	"testing"
	"time"

	"nba-data-service/internal/domain"
)

type fakeProvider struct {
	dates []string
}

func (p *fakeProvider) FetchGames(ctx context.Context, date string, _ string) ([]domain.Game, error) {
	p.dates = append(p.dates, date)
	return []domain.Game{
		{ID: date + "-1", Provider: "stub"},
	}, nil
}

func TestSyncerBackfillsPastAndFuture(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer := NewWriter(t.TempDir(), 10000)
	now := time.Date(2024, 1, 10, 12, 0, 0, 0, time.UTC)
	provider := &fakeProvider{}
	cfg := SyncConfig{
		Enabled:    true,
		Days:       3,
		FutureDays: 2,
		Interval:   time.Nanosecond,
	}

	// Seed snapshots: yesterday (will still refresh), 2 days back (should skip),
	// and future +2 (should skip).
	writeSimpleSnapshot(t, writer, "2024-01-09")
	writeSimpleSnapshot(t, writer, "2024-01-08")
	writeSimpleSnapshot(t, writer, "2024-01-12")

	syncer := NewSyncer(provider, writer, cfg, nil)
	syncer.now = func() time.Time { return now }

	syncer.Run(ctx)
	cancel()

	expected := []string{"2024-01-10", "2024-01-09", "2024-01-11"}

	assertDatesEqual(t, provider.dates, expected)
	for _, date := range expected {
		requireSnapshotExists(t, writer, date)
	}
	// Ensure previously existing snapshots remain.
	requireSnapshotExists(t, writer, "2024-01-08")
	requireSnapshotExists(t, writer, "2024-01-12")
}

type disabledProvider struct{}

func (disabledProvider) FetchGames(ctx context.Context, date string, tz string) ([]domain.Game, error) {
	return nil, nil
}

func TestSyncerSkipsWhenDisabledOrNil(t *testing.T) {
	s := NewSyncer(nil, nil, SyncConfig{Enabled: false}, nil)
	s.Run(context.Background())

	s = NewSyncer(disabledProvider{}, nil, SyncConfig{Enabled: true}, nil)
	s.Run(context.Background())
}

func TestSyncerSleepRespectsContext(t *testing.T) {
	s := NewSyncer(nil, nil, SyncConfig{Enabled: true}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	s.sleep(ctx, time.Second)
	if time.Since(start) > 50*time.Millisecond {
		t.Fatalf("expected sleep to return quickly when context canceled")
	}
}

func TestHasSnapshotNilWriter(t *testing.T) {
	s := NewSyncer(nil, nil, SyncConfig{}, nil)
	if s.hasSnapshot("2024-01-01") {
		t.Fatalf("expected hasSnapshot to be false with nil writer")
	}
}

func TestBuildDatesSkipsExistingSnapshots(t *testing.T) {
	w := NewWriter(t.TempDir(), 10000)
	writeSimpleSnapshot(t, w, "2024-01-03") // past (beyond yesterday)
	writeSimpleSnapshot(t, w, "2024-01-06") // future

	s := NewSyncer(nil, w, SyncConfig{Enabled: true, Days: 5, FutureDays: 2}, nil)
	now := time.Date(2024, 1, 5, 10, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	dates := s.buildDates(s.now())

	want := map[string]bool{
		"2024-01-05": true, // today
		"2024-01-04": true, // yesterday
	}
	for _, d := range dates {
		if want[d] {
			delete(want, d)
		}
		if d == "2024-01-03" || d == "2024-01-06" {
			t.Fatalf("expected existing snapshots to be skipped, got %s", d)
		}
	}
	if len(want) != 0 {
		t.Fatalf("expected today and yesterday to be present, missing %v", want)
	}
}
