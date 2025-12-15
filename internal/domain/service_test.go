package domain

import "testing"

type stubStore struct {
	listResult []Game
	getResult  Game
	getOK      bool

	setCalls int
	setValue []Game
}

func (s *stubStore) ListGames() []Game {
	return s.listResult
}

func (s *stubStore) GetGame(id string) (Game, bool) {
	_ = id
	return s.getResult, s.getOK
}

func (s *stubStore) SetGames(games []Game) {
	s.setCalls++
	s.setValue = games
}

func TestServiceGames(t *testing.T) {
	store := &stubStore{
		listResult: []Game{{ID: "one"}, {ID: "two"}},
	}
	svc := NewService(store)

	games := svc.Games()
	if len(games) != 2 {
		t.Fatalf("expected 2 games, got %d", len(games))
	}
	if games[0].ID != "one" || games[1].ID != "two" {
		t.Fatalf("unexpected games returned: %+v", games)
	}
}

func TestServiceGameByID(t *testing.T) {
	want := Game{ID: "abc"}
	store := &stubStore{
		getResult: want,
		getOK:     true,
	}
	svc := NewService(store)

	got, ok := svc.GameByID("abc")
	if !ok {
		t.Fatalf("expected to find game")
	}
	if got.ID != want.ID {
		t.Fatalf("expected %s got %s", want.ID, got.ID)
	}
}

func TestServiceReplaceGames(t *testing.T) {
	store := &stubStore{}
	svc := NewService(store)

	payload := []Game{{ID: "replace-me"}}
	svc.ReplaceGames(payload)

	if store.setCalls != 1 {
		t.Fatalf("expected SetGames to be called once, got %d", store.setCalls)
	}
	if len(store.setValue) != 1 || store.setValue[0].ID != "replace-me" {
		t.Fatalf("unexpected SetGames payload: %+v", store.setValue)
	}
}
