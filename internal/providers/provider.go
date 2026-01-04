package providers

import (
	"context"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
)

// GameProvider defines how upstream game data is fetched and normalized.
// The date parameter, when provided, should be a YYYY-MM-DD string indicating which day's games to fetch.
// Providers should interpret an empty date as "today" in their configured timezone.
type GameProvider interface {
	FetchGames(ctx context.Context, date string, tz string) ([]domaingames.Game, error)
}
