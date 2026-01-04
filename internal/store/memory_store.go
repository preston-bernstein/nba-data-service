package store

import (
	"sync"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain/players"
	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
)

// MemoryStore keeps a thread-safe snapshot of games in memory.
type MemoryStore struct {
	mu      sync.RWMutex
	games   map[string]domaingames.Game
	teams   map[string]teams.Team
	players map[string]players.Player
	// Hooks for future metadata (e.g., last refreshed timestamps) if needed for activity filtering.
}

// NewMemoryStore constructs an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		games:   make(map[string]domaingames.Game),
		teams:   make(map[string]teams.Team),
		players: make(map[string]players.Player),
	}
}

// ListGames returns a copy of the current games slice.
func (s *MemoryStore) ListGames() []domaingames.Game {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]domaingames.Game, 0, len(s.games))
	for _, g := range s.games {
		result = append(result, g)
	}
	return result
}

// GetGame retrieves a game by ID.
func (s *MemoryStore) GetGame(id string) (domaingames.Game, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	g, ok := s.games[id]
	return g, ok
}

// SetGames replaces the existing games with a new snapshot.
func (s *MemoryStore) SetGames(games []domaingames.Game) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.games = make(map[string]domaingames.Game, len(games))
	for _, g := range games {
		s.games[g.ID] = g
	}
}

// ListTeams returns a copy of the current teams.
func (s *MemoryStore) ListTeams() []teams.Team {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]teams.Team, 0, len(s.teams))
	for _, t := range s.teams {
		result = append(result, t)
	}
	return result
}

// GetTeam retrieves a team by ID.
func (s *MemoryStore) GetTeam(id string) (teams.Team, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.teams[id]
	return t, ok
}

// SetTeams replaces the existing teams with a new snapshot.
func (s *MemoryStore) SetTeams(items []teams.Team) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.teams = make(map[string]teams.Team, len(items))
	for _, t := range items {
		s.teams[t.ID] = t
	}
}

// ListPlayers returns a copy of the current players.
func (s *MemoryStore) ListPlayers() []players.Player {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]players.Player, 0, len(s.players))
	for _, p := range s.players {
		result = append(result, p)
	}
	return result
}

// GetPlayer retrieves a player by ID.
func (s *MemoryStore) GetPlayer(id string) (players.Player, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	p, ok := s.players[id]
	return p, ok
}

// SetPlayers replaces the existing players with a new snapshot.
func (s *MemoryStore) SetPlayers(items []players.Player) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.players = make(map[string]players.Player, len(items))
	for _, p := range items {
		s.players[p.ID] = p
	}
}
