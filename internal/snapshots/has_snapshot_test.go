package snapshots

import "testing"

func TestHasSnapshotDetectsExistingFile(t *testing.T) {
	base := t.TempDir()
	w := NewWriter(base, 10000)
	writeSimpleSnapshot(t, w, "2024-01-01")

	s := NewSyncer(nil, w, SyncConfig{Enabled: true}, nil)
	if !s.hasSnapshot("2024-01-01") {
		t.Fatalf("expected hasSnapshot to detect existing file")
	}
	if s.hasSnapshot("2024-01-02") {
		t.Fatalf("expected hasSnapshot to be false for missing date")
	}
}
