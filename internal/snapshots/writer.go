package snapshots

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/timeutil"
)

type snapshotKind string

const (
	kindGames snapshotKind = "games"
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

func (w *Writer) snapshotPath(kind snapshotKind, date string, page int) string {
	switch kind {
	case kindGames:
		return GameSnapshotPath(w.basePath, date)
	default:
		return filepath.Join(w.basePath, string(kind), fmt.Sprintf("%s.json", date))
	}
}

// BasePath exposes the writer root path (primarily for testing).
func (w *Writer) BasePath() string {
	if w == nil {
		return ""
	}
	return w.basePath
}

// WriteGamesSnapshot writes the games snapshot for the given date (YYYY-MM-DD) and prunes old snapshots.
func (w *Writer) WriteGamesSnapshot(date string, snapshot domaingames.TodayResponse) error {
	if snapshot.Date == "" {
		snapshot.Date = date
	}
	sort.Slice(snapshot.Games, func(i, j int) bool {
		return snapshot.Games[i].ID < snapshot.Games[j].ID
	})
	return w.writeSnapshot(kindGames, date, snapshot)
}

func (w *Writer) writeSnapshot(kind snapshotKind, date string, payload any, page ...int) error {
	if w == nil {
		return fmt.Errorf("snapshot writer not configured")
	}
	if date == "" {
		return fmt.Errorf("date required")
	}

	pageNum := 0
	if len(page) > 0 {
		pageNum = page[0]
	}
	target := w.snapshotPath(kind, date, pageNum)
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}

	tmp := target + ".tmp"
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}

	if existing, err := os.ReadFile(target); err == nil && bytes.Equal(existing, data) {
		return w.updateManifest(kind, date)
	}

	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, target); err != nil {
		return err
	}

	return w.updateManifest(kind, date)
}

func (w *Writer) updateManifest(kind snapshotKind, date string) error {
	manifestPath := filepath.Join(w.basePath, "manifest.json")
	m, _ := readManifest(manifestPath, w.retentionDays)
	now := time.Now().UTC()

	dates, err := w.listDates(kind)
	if err != nil {
		return err
	}
	if !containsDate(dates, date) {
		dates = append(dates, date)
	}
	pruned, err := w.pruneOldSnapshots(kind, dates)
	if err != nil {
		return err
	}

	switch kind {
	case kindGames:
		m.Games.Dates = pruned
		m.Games.LastRefreshed = now
		m.Retention.GamesDays = w.retentionDays
	}

	return writeManifest(w.basePath, m)
}

func containsDate(dates []string, date string) bool {
	for _, d := range dates {
		if d == date {
			return true
		}
	}
	return false
}

func (w *Writer) listDates(kind snapshotKind) ([]string, error) {
	dir := filepath.Join(w.basePath, string(kind))
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}
	var (
		dates []string
		seen  = make(map[string]struct{})
	)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if filepath.Ext(name) != ".json" {
			continue
		}
		base := name[:len(name)-len(".json")]
		if _, ok := seen[base]; ok {
			continue
		}
		seen[base] = struct{}{}
		dates = append(dates, base)
	}
	sort.Strings(dates)
	return dates, nil
}

func (w *Writer) pruneOldSnapshots(kind snapshotKind, dates []string) ([]string, error) {
	now := time.Now().UTC()
	cutoff := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -w.retentionDays)
	var keep []string
	for _, d := range dates {
		parsed, err := timeutil.ParseDate(d)
		if err != nil {
			keep = append(keep, d)
			continue
		}
		if parsed.Before(cutoff) {
			path := w.snapshotPath(kind, d, 0)
			_ = os.Remove(path)
			continue
		}
		keep = append(keep, d)
	}
	sort.Strings(keep)
	return keep, nil
}
