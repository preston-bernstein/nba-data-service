package teams

import (
	"testing"

	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
)

type stubTeamStore struct {
	items []teams.Team
	byID  map[string]teams.Team
}

func (s *stubTeamStore) ListTeams() []teams.Team { return s.items }
func (s *stubTeamStore) GetTeam(id string) (teams.Team, bool) {
	val, ok := s.byID[id]
	return val, ok
}
func (s *stubTeamStore) SetTeams(items []teams.Team) { s.items = items }

func TestTeamsService(t *testing.T) {
	store := &stubTeamStore{
		items: []teams.Team{{ID: "t1"}},
		byID:  map[string]teams.Team{"t1": {ID: "t1"}},
	}
	svc := NewService(store)

	if len(svc.Teams()) != 1 {
		t.Fatalf("expected teams from store")
	}
	if _, ok := svc.TeamByID("t1"); !ok {
		t.Fatalf("expected team by id")
	}
	if len(svc.ActiveTeams()) != 1 {
		t.Fatalf("expected active teams to mirror teams")
	}

	svc.ReplaceTeams([]teams.Team{{ID: "t2"}})
	if len(store.items) != 1 || store.items[0].ID != "t2" {
		t.Fatalf("expected replace to set store items")
	}
}
