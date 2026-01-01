package snapshots

import (
	"context"
	"os"
	"path/filepath"
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

	tmp := t.TempDir()
	now := time.Date(2024, 1, 10, 12, 0, 0, 0, time.UTC)
	provider := &fakeProvider{}
	// Large retention to avoid pruning in test regardless of current date.
	writer := NewWriter(tmp, 10000)
	cfg := SyncConfig{
		Enabled:    true,
		Days:       3,
		FutureDays: 2,
		Interval:   time.Nanosecond,
	}

	// Seed snapshots: yesterday (will still refresh), 2 days back (should skip),
	// and future +2 (should skip).
	requireWrite(t, writer, "2024-01-09")
	requireWrite(t, writer, "2024-01-08")
	requireWrite(t, writer, "2024-01-12")

	syncer := NewSyncer(provider, writer, cfg, nil)
	syncer.now = func() time.Time { return now }

	syncer.Run(ctx)
	cancel()

	expected := []string{"2024-01-10", "2024-01-09", "2024-01-11"}

	if !equalSlices(provider.dates, expected) {
		t.Fatalf("provider dates mismatch: got %v, want %v", provider.dates, expected)
	}
	for _, date := range expected {
		requireSnapshotExists(t, tmp, date)
	}
	// Ensure previously existing snapshots remain.
	requireSnapshotExists(t, tmp, "2024-01-08")
	requireSnapshotExists(t, tmp, "2024-01-12")
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func requireSnapshotExists(t *testing.T, base, date string) {
	t.Helper()
	path := filepath.Join(base, "games", date+".json")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected snapshot for %s to be written: %v", date, err)
	}
}

func requireWrite(t *testing.T, w *Writer, date string) {
	t.Helper()
	err := w.WriteGamesSnapshot(date, domain.TodayResponse{
		Date: date,
		Games: []domain.Game{
			{ID: date, Provider: "stub"},
		},
	})
	if err != nil {
		t.Fatalf("seed snapshot failed for %s: %v", date, err)
	}
}
