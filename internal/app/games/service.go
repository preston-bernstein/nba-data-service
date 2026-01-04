package games

import "github.com/preston-bernstein/nba-data-service/internal/domain"

// Store defines the contract for persisting and retrieving games.
type Store interface {
	ListGames() []domain.Game
	GetGame(id string) (domain.Game, bool)
	SetGames(games []domain.Game)
}

// Service coordinates game operations using a Store.
type Service struct {
	store Store
}

// NewService constructs a Service with the provided Store.
func NewService(store Store) *Service {
	return &Service{store: store}
}

// Games returns the current set of games.
func (s *Service) Games() []domain.Game {
	return s.store.ListGames()
}

// GameByID returns a single game if present.
func (s *Service) GameByID(id string) (domain.Game, bool) {
	return s.store.GetGame(id)
}

// ReplaceGames swaps the in-memory games with a new snapshot.
func (s *Service) ReplaceGames(games []domain.Game) {
	s.store.SetGames(games)
}
