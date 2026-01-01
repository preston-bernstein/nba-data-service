package snapshots

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"nba-data-service/internal/domain"
)

func TestWriterWritesSnapshotAndManifest(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 10)

	today := time.Now().Format("2006-01-02")
	snap := domain.TodayResponse{
		Date:  today,
		Games: []domain.Game{{ID: "g1"}},
	}

	if err := w.WriteGamesSnapshot(today, snap); err != nil {
		t.Fatalf("write snapshot failed: %v", err)
	}

	// Verify snapshot file exists.
	data, err := os.ReadFile(filepath.Join(dir, "games", today+".json"))
	if err != nil {
		t.Fatalf("expected snapshot file, got err %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("expected snapshot content")
	}

	// Verify manifest was written.
	mBytes, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatalf("expected manifest, got err %v", err)
	}
	if len(mBytes) == 0 {
		t.Fatalf("expected manifest content")
	}
}

func TestWriterPrunesOldSnapshots(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 1) // 1-day retention

	oldDate := time.Now().AddDate(0, 0, -5).Format("2006-01-02")
	newDate := time.Now().Format("2006-01-02")

	// Write an old snapshot and a new one.
	for _, d := range []string{oldDate, newDate} {
		snap := domain.TodayResponse{
			Date:  d,
			Games: []domain.Game{{ID: d}},
		}
		if err := w.WriteGamesSnapshot(d, snap); err != nil {
			t.Fatalf("write snapshot %s failed: %v", d, err)
		}
	}

	// Old snapshot should be pruned.
	if _, err := os.Stat(filepath.Join(dir, "games", oldDate+".json")); err == nil {
		t.Fatalf("expected old snapshot to be pruned")
	}
	if _, err := os.Stat(filepath.Join(dir, "games", newDate+".json")); err != nil {
		t.Fatalf("expected new snapshot to exist")
	}
}

func TestWriterHandlesNilAndEmptyDate(t *testing.T) {
	var w *Writer
	if err := w.WriteGamesSnapshot("2024-01-01", domain.TodayResponse{}); err == nil {
		t.Fatalf("expected error for nil writer")
	}

	w = NewWriter(t.TempDir(), 1)
	if err := w.WriteGamesSnapshot("", domain.TodayResponse{}); err == nil {
		t.Fatalf("expected error for empty date")
	}
}
