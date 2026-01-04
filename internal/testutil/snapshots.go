package testutil

import (
	"testing"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
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
	if err := writeSnapshotPayload(w, date); err != nil {
		t.Fatalf("failed to write snapshot %s: %v", date, err)
	}
}

func writeSnapshotPayload(w *snapshots.Writer, date string) error {
	return w.WriteGamesSnapshot(date, domaingames.TodayResponse{
		Date: date,
		Games: []domaingames.Game{
			{ID: date},
		},
	})
}

// SnapshotPath returns the expected file path for a snapshot date.
func SnapshotPath(w *snapshots.Writer, date string) string {
	return snapshots.GameSnapshotPath(w.BasePath(), date)
}
