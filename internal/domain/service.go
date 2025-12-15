package domain

// Store defines the contract for persisting and retrieving games.
type Store interface {
	ListGames() []Game
	GetGame(id string) (Game, bool)
	SetGames(games []Game)
}

// Service coordinates domain operations using a Store.
type Service struct {
	store Store
}

// NewService constructs a Service with the provided Store.
func NewService(store Store) *Service {
	return &Service{store: store}
}

// Games returns the current set of games.
func (s *Service) Games() []Game {
	return s.store.ListGames()
}

// GameByID returns a single game if present.
func (s *Service) GameByID(id string) (Game, bool) {
	return s.store.GetGame(id)
}

// ReplaceGames swaps the in-memory games with a new snapshot.
func (s *Service) ReplaceGames(games []Game) {
	s.store.SetGames(games)
}
