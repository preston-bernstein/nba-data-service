package snapshots

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
)

func TestFSStoreLoadGames(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "games"), 0o755); err != nil {
		t.Fatalf("failed to create games dir: %v", err)
	}

	snap := domaingames.TodayResponse{Date: "2024-01-02", Games: []domaingames.Game{{ID: "g1"}}}
	data, _ := json.Marshal(snap)
	if err := os.WriteFile(filepath.Join(dir, "games", "2024-01-02.json"), data, 0o644); err != nil {
		t.Fatalf("failed to write games snapshot: %v", err)
	}

	store := NewFSStore(dir)
	got, err := store.LoadGames("2024-01-02")
	if err != nil {
		t.Fatalf("failed to load games: %v", err)
	}
	if got.Date != "2024-01-02" || len(got.Games) != 1 || got.Games[0].ID != "g1" {
		t.Fatalf("unexpected games snapshot: %+v", got)
	}
}

func TestFSStoreErrors(t *testing.T) {
	store := NewFSStore(t.TempDir())
	if _, err := store.LoadGames("2024-01-01"); err == nil {
		t.Fatalf("expected error for missing game snapshot")
	}
	if _, err := store.LoadGames(""); err == nil {
		t.Fatalf("expected error for empty date")
	}
	var nilStore *FSStore
	if _, err := nilStore.LoadGames("2024-01-01"); err == nil {
		t.Fatalf("expected error for nil store")
	}
}

func TestDecodeFileError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "games", "bad.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}
	if err := os.WriteFile(path, []byte("{bad json"), 0o644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
	store := NewFSStore(dir)
	if err := store.decodeFile(path, &domaingames.TodayResponse{}); err == nil {
		t.Fatalf("expected decode error")
	}
}
