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

func TestFSStoreFindGameByID(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "games"), 0o755); err != nil {
		t.Fatalf("failed to create games dir: %v", err)
	}

	snap := domaingames.TodayResponse{
		Date:  "2024-01-15",
		Games: []domaingames.Game{{ID: "game-1"}, {ID: "game-2"}},
	}
	data, _ := json.Marshal(snap)
	if err := os.WriteFile(filepath.Join(dir, "games", "2024-01-15.json"), data, 0o644); err != nil {
		t.Fatalf("failed to write snapshot: %v", err)
	}

	store := NewFSStore(dir)

	// Found case.
	g, ok := store.FindGameByID("2024-01-15", "game-2")
	if !ok {
		t.Fatalf("expected to find game-2")
	}
	if g.ID != "game-2" {
		t.Fatalf("unexpected game: %+v", g)
	}

	// Not found case.
	_, ok = store.FindGameByID("2024-01-15", "missing")
	if ok {
		t.Fatalf("expected not to find missing game")
	}

	// Missing snapshot case.
	_, ok = store.FindGameByID("2024-01-01", "game-1")
	if ok {
		t.Fatalf("expected not to find game in missing snapshot")
	}
}
