package snapshots

import (
	"encoding/json"
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

	writeSnapshot(t, w, today, snap)

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
		writeSnapshot(t, w, d, snap)
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

func TestNewWriterDefaultsRetention(t *testing.T) {
	w := NewWriter(t.TempDir(), 0)
	if w.retentionDays <= 0 {
		t.Fatalf("expected retention to default when non-positive provided")
	}
}

func TestListGameDatesIgnoresNonJSONAndDirs(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "games", "nested"), 0o755); err != nil {
		t.Fatalf("failed to create nested dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "games", "2024-01-01.json"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("failed to write snapshot: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "games", "ignore.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("failed to write extra file: %v", err)
	}

	w := NewWriter(dir, 1)
	dates, err := w.listGameDates()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(dates) != 1 || dates[0] != "2024-01-01" {
		t.Fatalf("expected only json snapshots, got %v", dates)
	}
}

func TestBasePathExposesRoot(t *testing.T) {
	base := t.TempDir()
	w := NewWriter(base, 1)
	if w.BasePath() != base {
		t.Fatalf("expected base path %s, got %s", base, w.BasePath())
	}
}

func TestSnapshotPathUsesDateAndBase(t *testing.T) {
	base := t.TempDir()
	w := NewWriter(base, 1)
	path := w.snapshotPath("2024-01-01")
	if path == "" || path == base {
		t.Fatalf("expected snapshot path to include games file, got %s", path)
	}
}

func TestWriterHandlesNilOtel(t *testing.T) {
	w := NewWriter(t.TempDir(), 1)
	// Should not panic when otel instruments are nil inside recorder; covered via WriteGamesSnapshot path.
	if err := w.WriteGamesSnapshot("2024-02-02", domain.TodayResponse{}); err != nil {
		t.Fatalf("expected write to succeed, got %v", err)
	}
}

func TestListGameDatesMissingDirReturnsEmpty(t *testing.T) {
	w := NewWriter(t.TempDir(), 1)
	dates, err := w.listGameDates()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(dates) != 0 {
		t.Fatalf("expected empty dates, got %v", dates)
	}
}

func TestPruneOldSnapshotsKeepsRecent(t *testing.T) {
	w := NewWriter(t.TempDir(), 1)
	today := time.Now().Format("2006-01-02")
	old := time.Now().AddDate(0, 0, -10).Format("2006-01-02")
	writeSnapshot(t, w, today, simpleSnapshot(today))
	writeSnapshot(t, w, old, simpleSnapshot(old))
	keep, err := w.pruneOldSnapshots([]string{today, old})
	if err != nil {
		t.Fatalf("expected prune to succeed: %v", err)
	}
	if len(keep) != 1 || keep[0] != today {
		t.Fatalf("expected only today's snapshot kept, got %v", keep)
	}
}

func TestPruneOldSnapshotsKeepsUnparsableDates(t *testing.T) {
	w := NewWriter(t.TempDir(), 1)
	keep, err := w.pruneOldSnapshots([]string{"bad-date"})
	if err != nil {
		t.Fatalf("expected prune to succeed: %v", err)
	}
	if len(keep) != 1 || keep[0] != "bad-date" {
		t.Fatalf("expected unparsable date kept, got %v", keep)
	}
}

func TestManifestWrittenWithRetentionAndDates(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 3)
	date := "2024-03-03"
	if err := w.WriteGamesSnapshot(date, simpleSnapshot(date)); err != nil {
		t.Fatalf("write snapshot failed: %v", err)
	}
	m, err := readManifest(filepath.Join(dir, "manifest.json"), 0)
	if err != nil {
		t.Fatalf("expected manifest read: %v", err)
	}
	if m.Retention.GamesDays != 3 {
		t.Fatalf("expected retention 3, got %d", m.Retention.GamesDays)
	}
	if m.Games.LastRefreshed.IsZero() {
		t.Fatalf("expected lastRefreshed to be set")
	}
}

func TestBasePathNilWriter(t *testing.T) {
	var w *Writer
	if w.BasePath() != "" {
		t.Fatalf("expected empty base path for nil writer")
	}
}

func TestWriteGamesSnapshotWriteFileError(t *testing.T) {
	dir := t.TempDir()
	gamesDir := filepath.Join(dir, "games")
	if err := os.MkdirAll(gamesDir, 0o555); err != nil {
		t.Fatalf("failed to create games dir: %v", err)
	}
	w := NewWriter(dir, 1)
	if err := w.WriteGamesSnapshot("2024-01-01", simpleSnapshot("2024-01-01")); err == nil {
		t.Fatalf("expected write error when games dir is read-only")
	}
}

func TestWriteGamesSnapshotRenameError(t *testing.T) {
	dir := t.TempDir()
	// Create a directory at the target path to force rename to fail.
	targetDir := filepath.Join(dir, "games", "2024-01-01.json")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("failed to create target dir: %v", err)
	}
	w := NewWriter(dir, 1)
	if err := w.WriteGamesSnapshot("2024-01-01", simpleSnapshot("2024-01-01")); err == nil {
		t.Fatalf("expected rename error when target is a directory")
	}
}

func TestListGameDatesErrorWhenGamesNotDir(t *testing.T) {
	dir := t.TempDir()
	// Create a file named "games" to trigger ReadDir error (not IsNotExist).
	if err := os.WriteFile(filepath.Join(dir, "games"), []byte("not a dir"), 0o644); err != nil {
		t.Fatalf("failed to create games file: %v", err)
	}
	w := NewWriter(dir, 1)
	if _, err := w.listGameDates(); err == nil {
		t.Fatalf("expected error when games path is a file")
	}
}

func TestWriteGamesSnapshotSetsDateWhenMissing(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 10000)
	snap := domain.TodayResponse{Games: []domain.Game{{ID: "g1"}}}
	if err := w.WriteGamesSnapshot("2024-04-04", snap); err != nil {
		t.Fatalf("write snapshot failed: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "games", "2024-04-04.json"))
	if err != nil {
		t.Fatalf("expected snapshot file: %v", err)
	}
	var loaded domain.TodayResponse
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("failed to decode snapshot: %v", err)
	}
	if loaded.Date != "2024-04-04" {
		t.Fatalf("expected date to be set, got %s", loaded.Date)
	}
}
