package snapshots

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	domainplayers "github.com/preston-bernstein/nba-data-service/internal/domain/players"
	domainteams "github.com/preston-bernstein/nba-data-service/internal/domain/teams"
)

func TestFSStoreLoadsGamesSnapshot(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "games"), 0o755); err != nil {
		t.Fatalf("failed to create games dir: %v", err)
	}
	snap := domaingames.TodayResponse{
		Date: "2024-01-02",
		Games: []domaingames.Game{
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

func TestFSStoreTeamsAndPlayersRequireDateAndStore(t *testing.T) {
	store := NewFSStore(t.TempDir())
	if _, err := store.LoadTeams(""); err == nil {
		t.Fatalf("expected error for empty team date")
	}
	if _, err := store.LoadPlayers(""); err == nil {
		t.Fatalf("expected error for empty player date")
	}
	var nilStore *FSStore
	if _, err := nilStore.LoadTeams("2024-01-01"); err == nil {
		t.Fatalf("expected error for nil store teams")
	}
	if _, err := nilStore.LoadPlayers("2024-01-01"); err == nil {
		t.Fatalf("expected error for nil store players")
	}
}

func TestFSStoreSetsDateWhenMissingForTeamsAndPlayers(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "teams"), 0o755); err != nil {
		t.Fatalf("failed to create teams dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "players"), 0o755); err != nil {
		t.Fatalf("failed to create players dir: %v", err)
	}

	teamSnap := TeamsSnapshot{Teams: []domainteams.Team{{ID: "t1"}}} // no date set
	teamData, _ := json.Marshal(teamSnap)
	if err := os.WriteFile(filepath.Join(dir, "teams", "2024-02-02.json"), teamData, 0o644); err != nil {
		t.Fatalf("failed to write teams snapshot: %v", err)
	}

	playerSnap := PlayersSnapshot{Players: []domainplayers.Player{{ID: "p1", Team: domainteams.Team{ID: "t1"}}}} // no date set
	playerData, _ := json.Marshal(playerSnap)
	if err := os.WriteFile(filepath.Join(dir, "players", "2024-02-02.json"), playerData, 0o644); err != nil {
		t.Fatalf("failed to write players snapshot: %v", err)
	}

	store := NewFSStore(dir)
	teamsLoaded, err := store.LoadTeams("2024-02-02")
	if err != nil {
		t.Fatalf("expected teams snapshot load: %v", err)
	}
	if teamsLoaded.Date != "2024-02-02" {
		t.Fatalf("expected teams date set from filename, got %s", teamsLoaded.Date)
	}

	playersLoaded, err := store.LoadPlayers("2024-02-02")
	if err != nil {
		t.Fatalf("expected players snapshot load: %v", err)
	}
	if playersLoaded.Date != "2024-02-02" {
		t.Fatalf("expected players date set from filename, got %s", playersLoaded.Date)
	}
}

func TestFSStoreLoadsTeamsAndPlayers(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "teams"), 0o755); err != nil {
		t.Fatalf("failed to create teams dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "players"), 0o755); err != nil {
		t.Fatalf("failed to create players dir: %v", err)
	}

	teamsSnap := TeamsSnapshot{Teams: []domainteams.Team{{ID: "t1"}}}
	teamData, _ := json.Marshal(teamsSnap)
	if err := os.WriteFile(filepath.Join(dir, "teams", "2024-01-02.json"), teamData, 0o644); err != nil {
		t.Fatalf("failed to write teams snapshot: %v", err)
	}

	playersSnap := PlayersSnapshot{Players: []domainplayers.Player{{ID: "p1", Team: domainteams.Team{ID: "t1"}}}}
	playerData, _ := json.Marshal(playersSnap)
	if err := os.WriteFile(filepath.Join(dir, "players", "2024-01-02.json"), playerData, 0o644); err != nil {
		t.Fatalf("failed to write players snapshot: %v", err)
	}

	store := NewFSStore(dir)
	teams, err := store.LoadTeams("2024-01-02")
	if err != nil {
		t.Fatalf("failed to load teams: %v", err)
	}
	if teams.Date != "2024-01-02" || len(teams.Teams) != 1 {
		t.Fatalf("unexpected teams snapshot: %+v", teams)
	}

	players, err := store.LoadPlayers("2024-01-02")
	if err != nil {
		t.Fatalf("failed to load players: %v", err)
	}
	if players.Date != "2024-01-02" || len(players.Players) != 1 {
		t.Fatalf("unexpected players snapshot: %+v", players)
	}
}

func TestFSStoreMissingTeamOrPlayerSnapshot(t *testing.T) {
	store := NewFSStore(t.TempDir())
	if _, err := store.LoadTeams("2024-01-01"); err == nil {
		t.Fatalf("expected error for missing team snapshot")
	}
	if _, err := store.LoadPlayers("2024-01-01"); err == nil {
		t.Fatalf("expected error for missing player snapshot")
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
	snap := domaingames.TodayResponse{
		Games: []domaingames.Game{{ID: "g1"}},
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

func TestFSStoreDecodeError(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "games"), 0o755); err != nil {
		t.Fatalf("failed to create games dir: %v", err)
	}
	path := filepath.Join(dir, "games", "2024-03-02.json")
	if err := os.WriteFile(path, []byte("{bad json"), 0o644); err != nil {
		t.Fatalf("failed to write snapshot: %v", err)
	}
	store := NewFSStore(dir)
	if _, err := store.LoadGames("2024-03-02"); err == nil {
		t.Fatalf("expected decode error")
	}
}
