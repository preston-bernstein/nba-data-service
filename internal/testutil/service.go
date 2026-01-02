package testutil

import (
	"nba-data-service/internal/app/games"
	"nba-data-service/internal/domain"
	"nba-data-service/internal/store"
)

// NewServiceWithGames builds a games service backed by an in-memory store preloaded with games.
func NewServiceWithGames(g []domain.Game) *games.Service {
	ms := store.NewMemoryStore()
	if len(g) > 0 {
		ms.SetGames(g)
	}
	return games.NewService(ms)
}
