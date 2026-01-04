package snapshots

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/preston-bernstein/nba-data-service/internal/domain"
)

// Writer persists snapshots and manifest with pruning.
type Writer struct {
	basePath      string
	retentionDays int
}

// NewWriter constructs a writer rooted at basePath with a rolling window retention.
func NewWriter(basePath string, retentionDays int) *Writer {
	if retentionDays <= 0 {
		retentionDays = 14
	}
	return &Writer{
		basePath:      basePath,
		retentionDays: retentionDays,
	}
}

func (w *Writer) snapshotPath(date string) string {
	return filepath.Join(w.basePath, "games", fmt.Sprintf("%s.json", date))
}

// BasePath exposes the writer root path (primarily for testing).
func (w *Writer) BasePath() string {
	if w == nil {
		return ""
	}
	return w.basePath
}

// WriteGamesSnapshot writes the games snapshot for the given date (YYYY-MM-DD) and prunes old snapshots.
func (w *Writer) WriteGamesSnapshot(date string, snapshot domain.TodayResponse) error {
	if w == nil {
		return fmt.Errorf("snapshot writer not configured")
	}
	if date == "" {
		return fmt.Errorf("date required")
	}
	if snapshot.Date == "" {
		snapshot.Date = date
	}
	target := w.snapshotPath(date)
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}

	// Write snapshot atomically.
	tmp := target + ".tmp"
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, target); err != nil {
		return err
	}

	// Update manifest and prune.
	manifestPath := filepath.Join(w.basePath, "manifest.json")
	m, _ := readManifest(manifestPath, w.retentionDays)
	m.Games.LastRefreshed = time.Now().UTC()
	m.Retention.GamesDays = w.retentionDays

	dates, err := w.listGameDates()
	if err != nil {
		return err
	}
	// Ensure current date is included.
	found := false
	for _, d := range dates {
		if d == date {
			found = true
			break
		}
	}
	if !found {
		dates = append(dates, date)
	}
	prunedDates, err := w.pruneOldSnapshots(dates)
	if err != nil {
		return err
	}
	m.Games.Dates = prunedDates
	if err := writeManifest(w.basePath, m); err != nil {
		return err
	}
	return nil
}

func (w *Writer) listGameDates() ([]string, error) {
	gamesDir := filepath.Join(w.basePath, "games")
	entries, err := os.ReadDir(gamesDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}
	var dates []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if filepath.Ext(name) != ".json" {
			continue
		}
		dates = append(dates, name[:len(name)-len(".json")])
	}
	sort.Strings(dates)
	return dates, nil
}

func (w *Writer) pruneOldSnapshots(dates []string) ([]string, error) {
	// Keep only dates within retentionDays from now (date-level, not time-of-day).
	now := time.Now().UTC()
	cutoff := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -w.retentionDays)
	var keep []string
	for _, d := range dates {
		parsed, err := time.Parse("2006-01-02", d)
		if err != nil {
			// If unparsable, keep it to avoid accidental deletes.
			keep = append(keep, d)
			continue
		}
		if parsed.Before(cutoff) {
			path := w.snapshotPath(d)
			_ = os.Remove(path)
			continue
		}
		keep = append(keep, d)
	}
	sort.Strings(keep)
	return keep, nil
}
