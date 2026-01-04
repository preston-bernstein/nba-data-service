package providers

import (
	"context"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain/players"
	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
)

// GameProvider defines how upstream game data is fetched and normalized.
// The date parameter, when provided, should be a YYYY-MM-DD string indicating which day's games to fetch.
// Providers should interpret an empty date as "today" in their configured timezone.
type GameProvider interface {
	FetchGames(ctx context.Context, date string, tz string) ([]domaingames.Game, error)
}

// TeamProvider fetches normalized teams.
type TeamProvider interface {
	FetchTeams(ctx context.Context) ([]teams.Team, error)
}

// PlayerProvider fetches normalized players.
type PlayerProvider interface {
	FetchPlayers(ctx context.Context) ([]players.Player, error)
}

// DataProvider combines all provider capabilities.
type DataProvider interface {
	GameProvider
	TeamProvider
	PlayerProvider
}
