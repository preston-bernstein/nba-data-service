package teams

import "github.com/preston-bernstein/nba-data-service/internal/domain/teams"

// Store defines the contract for persisting and retrieving teams.
type Store interface {
	ListTeams() []teams.Team
	GetTeam(id string) (teams.Team, bool)
	SetTeams([]teams.Team)
}

// Service coordinates team operations using a Store.
type Service struct {
	store Store
	// activeOnly currently returns all teams; hook for future activity filtering.
}

// ActiveTeams returns teams considered active (currently same as Teams; derived activity uses store state).
func (s *Service) ActiveTeams() []teams.Team {
	return s.Teams()
}

// NewService constructs a Service with the provided Store.
func NewService(store Store) *Service {
	return &Service{store: store}
}

// Teams returns the current set of teams.
func (s *Service) Teams() []teams.Team {
	return s.store.ListTeams()
}

// TeamByID returns a single team if present.
func (s *Service) TeamByID(id string) (teams.Team, bool) {
	return s.store.GetTeam(id)
}

// ReplaceTeams swaps the in-memory teams with a new snapshot.
func (s *Service) ReplaceTeams(items []teams.Team) {
	s.store.SetTeams(items)
}
