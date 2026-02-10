package snapshots

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/timeutil"
)

func TestWriterWritesSnapshotAndManifest(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 10)

	today := timeutil.FormatDate(time.Now())
	snap := domaingames.TodayResponse{
		Date:  today,
		Games: []domaingames.Game{{ID: "a"}, {ID: "b"}},
	}

	if err := w.WriteGamesSnapshot(today, snap); err != nil {
		t.Fatalf("write snapshot failed: %v", err)
	}

	// Verify file exists.
	_, err := os.Stat(filepath.Join(dir, "games", today+".json"))
	if err != nil {
		t.Fatalf("expected snapshot file: %v", err)
	}

	// Verify manifest updated.
	m, err := readManifest(filepath.Join(dir, "manifest.json"), 0)
	if err != nil {
		t.Fatalf("expected manifest read: %v", err)
	}
	if len(m.Games.Dates) != 1 || m.Games.Dates[0] != today {
		t.Fatalf("expected manifest dates to include today, got %+v", m.Games.Dates)
	}
	if m.Games.LastRefreshed.IsZero() {
		t.Fatalf("expected lastRefreshed to be set")
	}
}

func TestWriteGamesSnapshotSetsDateAndSorts(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 10000)
	date := timeutil.FormatDate(time.Now().UTC())
	gamesSnap := domaingames.TodayResponse{
		Games: []domaingames.Game{
			{ID: "b"},
			{ID: "a"},
		},
	}
	if err := w.WriteGamesSnapshot(date, gamesSnap); err != nil {
		t.Fatalf("write games snapshot failed: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "games", date+".json"))
	if err != nil {
		t.Fatalf("expected games snapshot file: %v", err)
	}
	var loaded domaingames.TodayResponse
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("failed to decode games snapshot: %v", err)
	}
	if loaded.Date != date || loaded.Games[0].ID != "a" {
		t.Fatalf("expected games sorted with date set, got %+v", loaded)
	}
}

func TestWriteSnapshotRequiresDateAndWriter(t *testing.T) {
	w := NewWriter(t.TempDir(), 7)
	if err := w.WriteGamesSnapshot("", domaingames.TodayResponse{}); err == nil {
		t.Fatalf("expected error for empty date")
	}
	var nilWriter *Writer
	if err := nilWriter.WriteGamesSnapshot("2024-01-01", domaingames.TodayResponse{}); err == nil {
		t.Fatalf("expected error for nil writer")
	}
}

func TestNewWriterDefaultsRetention(t *testing.T) {
	w := NewWriter(t.TempDir(), 0)
	date := timeutil.FormatDate(time.Now().UTC())
	if err := w.WriteGamesSnapshot(date, domaingames.TodayResponse{Games: []domaingames.Game{{ID: "g1"}}}); err != nil {
		t.Fatalf("expected snapshot write with default retention, got %v", err)
	}
	m, err := readManifest(filepath.Join(w.BasePath(), "manifest.json"), 0)
	if err != nil {
		t.Fatalf("expected manifest read: %v", err)
	}
	if m.Retention.GamesDays != 14 {
		t.Fatalf("expected default retention 14, got %d", m.Retention.GamesDays)
	}
}

func TestBasePathHandlesNil(t *testing.T) {
	var w *Writer
	if w.BasePath() != "" {
		t.Fatalf("expected empty base path for nil writer")
	}
}

func TestPruneOldSnapshotsRemovesExpiredAndKeepsInvalid(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 1)
	old := timeutil.FormatDate(time.Now().AddDate(0, 0, -3))
	recent := timeutil.FormatDate(time.Now())
	invalid := "not-a-date"

	writeFile := func(date string) {
		path := filepath.Join(dir, "games", date+".json")
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir failed: %v", err)
		}
		if err := os.WriteFile(path, []byte("{}"), 0o644); err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}
	writeFile(old)
	writeFile(recent)
	writeFile(invalid)

	pruned, err := w.pruneOldSnapshots(kindGames, []string{old, recent, invalid})
	if err != nil {
		t.Fatalf("prune failed: %v", err)
	}
	if len(pruned) != 2 {
		t.Fatalf("expected recent and invalid dates, got %v", pruned)
	}
	found := map[string]bool{}
	for _, d := range pruned {
		found[d] = true
	}
	if !found[recent] || !found[invalid] {
		t.Fatalf("expected to keep recent and invalid dates, got %v", pruned)
	}
	if _, err := os.Stat(filepath.Join(dir, "games", old+".json")); !os.IsNotExist(err) {
		t.Fatalf("expected old snapshot removed")
	}
}

func TestPruneExpiredGamesSnapshotsRemovesStaleAndUpdatesManifest(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 2)
	old := timeutil.FormatDate(time.Now().AddDate(0, 0, -3))
	recent := timeutil.FormatDate(time.Now())

	writeFile := func(date string) {
		path := filepath.Join(dir, "games", date+".json")
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir failed: %v", err)
		}
		if err := os.WriteFile(path, []byte("{}"), 0o644); err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}
	writeFile(old)
	writeFile(recent)

	if err := w.PruneExpiredGamesSnapshots(); err != nil {
		t.Fatalf("PruneExpiredGamesSnapshots failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "games", old+".json")); !os.IsNotExist(err) {
		t.Fatalf("expected expired snapshot removed")
	}
	if _, err := os.Stat(filepath.Join(dir, "games", recent+".json")); err != nil {
		t.Fatalf("expected recent snapshot kept: %v", err)
	}
	m, _ := readManifest(filepath.Join(dir, "manifest.json"), 0)
	if len(m.Games.Dates) != 1 || m.Games.Dates[0] != recent {
		t.Fatalf("expected manifest to list only recent date, got %v", m.Games.Dates)
	}
}

func TestPruneExpiredGamesSnapshotsNilWriter(t *testing.T) {
	var w *Writer
	if err := w.PruneExpiredGamesSnapshots(); err == nil {
		t.Fatalf("expected error for nil writer")
	}
}

func TestDeleteAllGamesSnapshotsRemovesAllAndClearsManifest(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 2)
	dates := []string{"2024-01-01", "2024-01-02", "2024-01-03"}
	for _, d := range dates {
		path := filepath.Join(dir, "games", d+".json")
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir failed: %v", err)
		}
		if err := os.WriteFile(path, []byte("{}"), 0o644); err != nil {
			t.Fatalf("write failed: %v", err)
		}
	}
	if err := w.DeleteAllGamesSnapshots(); err != nil {
		t.Fatalf("DeleteAllGamesSnapshots failed: %v", err)
	}
	for _, d := range dates {
		if _, err := os.Stat(filepath.Join(dir, "games", d+".json")); !os.IsNotExist(err) {
			t.Fatalf("expected snapshot %s removed", d)
		}
	}
	m, _ := readManifest(filepath.Join(dir, "manifest.json"), 0)
	if len(m.Games.Dates) != 0 {
		t.Fatalf("expected manifest dates empty, got %v", m.Games.Dates)
	}
}

func TestDeleteAllGamesSnapshotsNilWriter(t *testing.T) {
	var w *Writer
	if err := w.DeleteAllGamesSnapshots(); err == nil {
		t.Fatalf("expected error for nil writer")
	}
}

func TestListDatesIgnoresNonJSONAndDirs(t *testing.T) {
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
	dates, err := w.listDates(kindGames)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(dates) != 1 || dates[0] != "2024-01-01" {
		t.Fatalf("expected only json snapshots, got %v", dates)
	}
}

func TestWriteSnapshotHandlesUnwritableDir(t *testing.T) {
	w := NewWriter("/root/invalid", 1) // likely unwritable in tests
	err := w.WriteGamesSnapshot("2024-01-01", domaingames.TodayResponse{})
	if err == nil {
		t.Fatalf("expected error for unwritable base path")
	}
}

func TestSnapshotPathHandlesUnknownKind(t *testing.T) {
	w := NewWriter(t.TempDir(), 7)
	path := w.snapshotPath(snapshotKind("other"), "2024-01-01", 0)
	if !strings.Contains(path, "other") || !strings.HasSuffix(path, "2024-01-01.json") {
		t.Fatalf("expected fallback path for unknown kind, got %s", path)
	}
}

func TestContainsDate(t *testing.T) {
	dates := []string{"2024-01-01", "2024-01-02"}
	if !containsDate(dates, "2024-01-01") {
		t.Fatalf("expected containsDate to find value")
	}
	if containsDate(dates, "2024-01-03") {
		t.Fatalf("expected containsDate to return false for missing date")
	}
}

func TestDeleteAllGamesSnapshotsReturnsErrorWhenListDatesFails(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "games"), []byte("x"), 0o644); err != nil {
		t.Fatalf("failed to create games file: %v", err)
	}
	w := NewWriter(dir, 7)
	if err := w.DeleteAllGamesSnapshots(); err == nil {
		t.Fatalf("expected error when games path is not a directory")
	}
}

func TestPruneExpiredGamesSnapshotsReturnsErrorWhenListDatesFails(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "games"), []byte("x"), 0o644); err != nil {
		t.Fatalf("failed to create games file: %v", err)
	}
	w := NewWriter(dir, 7)
	if err := w.PruneExpiredGamesSnapshots(); err == nil {
		t.Fatalf("expected error when games path is not a directory")
	}
}

func TestUpdateManifestReturnsErrorWhenListDatesFails(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "games"), []byte("x"), 0o644); err != nil {
		t.Fatalf("failed to create games file: %v", err)
	}
	w := NewWriter(dir, 7)
	if err := w.updateManifest(kindGames, "2024-01-01"); err == nil {
		t.Fatalf("expected updateManifest to return error")
	}
}

func TestUpdateManifestAddsDateWhenMissing(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 10000)
	date := timeutil.FormatDate(time.Now().UTC())
	if err := w.updateManifest(kindGames, date); err != nil {
		t.Fatalf("updateManifest failed: %v", err)
	}
	m, err := readManifest(filepath.Join(dir, "manifest.json"), 0)
	if err != nil {
		t.Fatalf("expected manifest read: %v", err)
	}
	if len(m.Games.Dates) != 1 || m.Games.Dates[0] != date {
		t.Fatalf("expected manifest dates to include %s, got %v", date, m.Games.Dates)
	}
}

func TestWriteSnapshotSkipsRewriteWhenUnchanged(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 10000)
	date := timeutil.FormatDate(time.Now().UTC())
	payload := map[string]string{"id": "same"}

	if err := w.writeSnapshot(kindGames, date, payload); err != nil {
		t.Fatalf("first write failed: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, "manifest.json")); err != nil && !os.IsNotExist(err) {
		t.Fatalf("failed to remove manifest: %v", err)
	}
	if err := w.writeSnapshot(kindGames, date, payload); err != nil {
		t.Fatalf("second write failed: %v", err)
	}
	m, err := readManifest(filepath.Join(dir, "manifest.json"), 0)
	if err != nil {
		t.Fatalf("expected manifest read: %v", err)
	}
	if len(m.Games.Dates) != 1 || m.Games.Dates[0] != date {
		t.Fatalf("expected manifest dates to include %s, got %v", date, m.Games.Dates)
	}
}

func TestWriteSnapshotWithPageParam(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 7)
	if err := w.writeSnapshot(kindGames, "2024-01-04", map[string]string{"id": "p1"}, 1); err != nil {
		t.Fatalf("writeSnapshot with page failed: %v", err)
	}
}

func TestWriteSnapshotMarshalError(t *testing.T) {
	w := NewWriter(t.TempDir(), 7)
	payload := make(chan int)
	if err := w.writeSnapshot(kindGames, "2024-01-05", payload); err == nil {
		t.Fatalf("expected marshal error")
	}
}

func TestWriteSnapshotWriteFileError(t *testing.T) {
	dir := t.TempDir()
	gamesDir := filepath.Join(dir, "games")
	if err := os.MkdirAll(gamesDir, 0o500); err != nil {
		t.Fatalf("failed to create games dir: %v", err)
	}
	w := NewWriter(dir, 7)
	if err := w.writeSnapshot(kindGames, "2024-01-06", map[string]string{"id": "nope"}); err == nil {
		t.Fatalf("expected write error for read-only directory")
	}
}

func TestWriteSnapshotRenameError(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "games", "2024-01-07.json")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("failed to create target dir: %v", err)
	}
	w := NewWriter(dir, 7)
	if err := w.writeSnapshot(kindGames, "2024-01-07", map[string]string{"id": "rename"}); err == nil {
		t.Fatalf("expected rename error when target is a directory")
	}
}
