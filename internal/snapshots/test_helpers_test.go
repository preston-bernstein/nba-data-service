package snapshots

import (
	"os"
	"path/filepath"
	"testing"

	"nba-data-service/internal/domain"
)

func simpleSnapshot(date string) domain.TodayResponse {
	return domain.TodayResponse{
		Date: date,
		Games: []domain.Game{
			{ID: date},
		},
	}
}

func writeSnapshot(t *testing.T, w *Writer, date string, snap domain.TodayResponse) {
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
