package providers

import (
	"context"

	"nba-games-service/internal/domain"
)

// GameProvider defines how upstream game data is fetched and normalized.
type GameProvider interface {
	FetchGames(ctx context.Context) ([]domain.Game, error)
}
