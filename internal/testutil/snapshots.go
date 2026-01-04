package testutil

import (
	"path/filepath"
	"testing"

	"github.com/preston-bernstein/nba-data-service/internal/domain"
	"github.com/preston-bernstein/nba-data-service/internal/snapshots"
)

// NewTempWriter returns a snapshot writer rooted in a temp dir.
func NewTempWriter(t *testing.T, retention int) *snapshots.Writer {
	t.Helper()
	return snapshots.NewWriter(t.TempDir(), retention)
}

// WriteSnapshot writes a snapshot with a single game for the date.
func WriteSnapshot(t *testing.T, w *snapshots.Writer, date string) {
	t.Helper()
	if err := w.WriteGamesSnapshot(date, domain.TodayResponse{
		Date: date,
		Games: []domain.Game{
			{ID: date},
		},
	}); err != nil {
		t.Fatalf("failed to write snapshot %s: %v", date, err)
	}
}

// SnapshotPath returns the expected file path for a snapshot date.
func SnapshotPath(w *snapshots.Writer, date string) string {
	return filepath.Join(w.BasePath(), "games", date+".json")
}
