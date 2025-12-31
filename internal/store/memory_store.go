package store

import (
	"sync"

	"nba-data-service/internal/domain"
)

// MemoryStore keeps a thread-safe snapshot of games in memory.
type MemoryStore struct {
	mu    sync.RWMutex
	games map[string]domain.Game
}

// NewMemoryStore constructs an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		games: make(map[string]domain.Game),
	}
}

// ListGames returns a copy of the current games slice.
func (s *MemoryStore) ListGames() []domain.Game {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]domain.Game, 0, len(s.games))
	for _, g := range s.games {
		result = append(result, g)
	}
	return result
}

// GetGame retrieves a game by ID.
func (s *MemoryStore) GetGame(id string) (domain.Game, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	g, ok := s.games[id]
	return g, ok
}

// SetGames replaces the existing games with a new snapshot.
func (s *MemoryStore) SetGames(games []domain.Game) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.games = make(map[string]domain.Game, len(games))
	for _, g := range games {
		s.games[g.ID] = g
	}
}
