package store

import (
	"testing"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain/players"
	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
)

func TestMemoryStoreSetAndGet(t *testing.T) {
	s := NewMemoryStore()

	games := []domaingames.Game{
		{ID: "1", Provider: "test"},
		{ID: "2", Provider: "test"},
	}

	s.SetGames(games)

	if got := len(s.ListGames()); got != 2 {
		t.Fatalf("expected 2 games, got %d", got)
	}

	game, ok := s.GetGame("1")
	if !ok {
		t.Fatalf("expected to find game with id 1")
	}
	if game.Provider != "test" {
		t.Fatalf("unexpected provider %s", game.Provider)
	}
}

func TestMemoryStoreGetNotFound(t *testing.T) {
	s := NewMemoryStore()
	if _, ok := s.GetGame("missing"); ok {
		t.Fatalf("expected missing id to return false")
	}
}

func TestMemoryStoreSetReplacesSnapshot(t *testing.T) {
	s := NewMemoryStore()
	s.SetGames([]domaingames.Game{{ID: "old"}})

	s.SetGames([]domaingames.Game{{ID: "new"}})

	if _, ok := s.GetGame("old"); ok {
		t.Fatalf("expected old game to be removed after replace")
	}
	if _, ok := s.GetGame("new"); !ok {
		t.Fatalf("expected new game to be present")
	}
}

func TestMemoryStoreListReturnsCopy(t *testing.T) {
	s := NewMemoryStore()
	s.SetGames([]domaingames.Game{{ID: "copy", Provider: "original"}})

	list := s.ListGames()
	if len(list) != 1 {
		t.Fatalf("expected 1 game, got %d", len(list))
	}

	list[0].Provider = "mutated"

	game, ok := s.GetGame("copy")
	if !ok {
		t.Fatalf("expected to find game")
	}
	if game.Provider != "original" {
		t.Fatalf("expected store to remain unchanged, got %s", game.Provider)
	}
}

func TestMemoryStoreTeams(t *testing.T) {
	s := NewMemoryStore()
	team1 := teams.Team{ID: "t1", Name: "Team 1"}
	team2 := teams.Team{ID: "t2", Name: "Team 2"}
	s.SetTeams([]teams.Team{team1, team2})

	if got := len(s.ListTeams()); got != 2 {
		t.Fatalf("expected 2 teams, got %d", got)
	}
	if got, ok := s.GetTeam("t2"); !ok || got.Name != "Team 2" {
		t.Fatalf("expected to retrieve team 2")
	}

	s.SetTeams([]teams.Team{{ID: "t3", Name: "Team 3"}})
	if _, ok := s.GetTeam("t1"); ok {
		t.Fatalf("expected old teams to be replaced")
	}
}

func TestMemoryStorePlayers(t *testing.T) {
	s := NewMemoryStore()
	p1 := players.Player{ID: "p1", FirstName: "A"}
	p2 := players.Player{ID: "p2", FirstName: "B"}
	s.SetPlayers([]players.Player{p1, p2})

	if got := len(s.ListPlayers()); got != 2 {
		t.Fatalf("expected 2 players, got %d", got)
	}
	if got, ok := s.GetPlayer("p1"); !ok || got.FirstName != "A" {
		t.Fatalf("expected to retrieve player p1")
	}

	s.SetPlayers([]players.Player{{ID: "p3", FirstName: "C"}})
	if _, ok := s.GetPlayer("p2"); ok {
		t.Fatalf("expected old players to be replaced")
	}
}
