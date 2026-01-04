package store

import (
	"testing"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
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
