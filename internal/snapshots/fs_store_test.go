package snapshots

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"nba-data-service/internal/domain"
)

func TestFSStoreLoadsGamesSnapshot(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "games"), 0o755); err != nil {
		t.Fatalf("failed to create games dir: %v", err)
	}
	snap := domain.TodayResponse{
		Date: "2024-01-02",
		Games: []domain.Game{
			{ID: "g1"},
		},
	}
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("failed to marshal snapshot: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "games", "2024-01-02.json"), data, 0o644); err != nil {
		t.Fatalf("failed to write snapshot: %v", err)
	}

	store := NewFSStore(dir)
	loaded, err := store.LoadGames("2024-01-02")
	if err != nil {
		t.Fatalf("failed to load snapshot: %v", err)
	}
	if loaded.Date != "2024-01-02" {
		t.Fatalf("expected date propagated, got %s", loaded.Date)
	}
	if len(loaded.Games) != 1 || loaded.Games[0].ID != "g1" {
		t.Fatalf("unexpected games %+v", loaded.Games)
	}
}

func TestFSStoreMissingSnapshotReturnsError(t *testing.T) {
	store := NewFSStore(t.TempDir())
	if _, err := store.LoadGames("2024-01-01"); err == nil {
		t.Fatalf("expected error for missing snapshot")
	}
}

func TestFSStoreRequiresDate(t *testing.T) {
	store := NewFSStore(t.TempDir())
	if _, err := store.LoadGames(""); err == nil {
		t.Fatalf("expected error for empty date")
	}
}

func TestFSStoreNilReceiver(t *testing.T) {
	var store *FSStore
	if _, err := store.LoadGames("2024-01-01"); err == nil {
		t.Fatalf("expected error for nil store")
	}
}

func TestFSStoreSetsDateWhenMissing(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "games"), 0o755); err != nil {
		t.Fatalf("failed to create games dir: %v", err)
	}
	// Snapshot without date field
	snap := domain.TodayResponse{
		Games: []domain.Game{{ID: "g1"}},
	}
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("failed to marshal snapshot: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "games", "2024-03-01.json"), data, 0o644); err != nil {
		t.Fatalf("failed to write snapshot: %v", err)
	}

	store := NewFSStore(dir)
	loaded, err := store.LoadGames("2024-03-01")
	if err != nil {
		t.Fatalf("failed to load snapshot: %v", err)
	}
	if loaded.Date != "2024-03-01" {
		t.Fatalf("expected date to be set from filename, got %s", loaded.Date)
	}
}
