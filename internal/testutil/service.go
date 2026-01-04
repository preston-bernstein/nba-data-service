package testutil

import (
	"github.com/preston-bernstein/nba-data-service/internal/app/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain"
	"github.com/preston-bernstein/nba-data-service/internal/store"
)

// NewServiceWithGames builds a games service backed by an in-memory store preloaded with games.
func NewServiceWithGames(g []domain.Game) *games.Service {
	ms := store.NewMemoryStore()
	if len(g) > 0 {
		ms.SetGames(g)
	}
	return games.NewService(ms)
}
