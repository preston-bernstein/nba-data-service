package players

import (
	"testing"

	"github.com/preston-bernstein/nba-data-service/internal/domain/players"
)

type stubPlayerStore struct {
	items []players.Player
	byID  map[string]players.Player
}

func (s *stubPlayerStore) ListPlayers() []players.Player { return s.items }
func (s *stubPlayerStore) GetPlayer(id string) (players.Player, bool) {
	val, ok := s.byID[id]
	return val, ok
}
func (s *stubPlayerStore) SetPlayers(items []players.Player) { s.items = items }

func TestPlayersService(t *testing.T) {
	store := &stubPlayerStore{
		items: []players.Player{{ID: "p1"}},
		byID:  map[string]players.Player{"p1": {ID: "p1"}},
	}
	svc := NewService(store)

	if len(svc.Players()) != 1 {
		t.Fatalf("expected players from store")
	}
	if len(svc.ActivePlayers()) != 1 {
		t.Fatalf("expected active players to mirror players")
	}
	if _, ok := svc.PlayerByID("p1"); !ok {
		t.Fatalf("expected player by id")
	}

	svc.ReplacePlayers([]players.Player{{ID: "p2"}})
	if len(store.items) != 1 || store.items[0].ID != "p2" {
		t.Fatalf("expected replace to set store items")
	}
}
