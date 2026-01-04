package players

import "github.com/preston-bernstein/nba-data-service/internal/domain/players"

// Store defines the contract for persisting and retrieving players.
type Store interface {
	ListPlayers() []players.Player
	GetPlayer(id string) (players.Player, bool)
	SetPlayers([]players.Player)
}

// Service coordinates player operations using a Store.
type Service struct {
	store Store
	// activeOnly currently returns all players; hook for future activity filtering.
}

// ActivePlayers returns players considered active (currently same as Players; derived activity uses store state).
func (s *Service) ActivePlayers() []players.Player {
	return s.Players()
}

// NewService constructs a Service with the provided Store.
func NewService(store Store) *Service {
	return &Service{store: store}
}

// Players returns the current set of players.
func (s *Service) Players() []players.Player {
	return s.store.ListPlayers()
}

// PlayerByID returns a single player if present.
func (s *Service) PlayerByID(id string) (players.Player, bool) {
	return s.store.GetPlayer(id)
}

// ReplacePlayers swaps the in-memory players with a new snapshot.
func (s *Service) ReplacePlayers(items []players.Player) {
	s.store.SetPlayers(items)
}
