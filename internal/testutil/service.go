package testutil

import (
	"github.com/preston-bernstein/nba-data-service/internal/app/games"
	appplayers "github.com/preston-bernstein/nba-data-service/internal/app/players"
	appteams "github.com/preston-bernstein/nba-data-service/internal/app/teams"
	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	domainplayers "github.com/preston-bernstein/nba-data-service/internal/domain/players"
	domainteams "github.com/preston-bernstein/nba-data-service/internal/domain/teams"
	"github.com/preston-bernstein/nba-data-service/internal/store"
)

// NewServiceWithGames builds a games service backed by an in-memory store preloaded with games.
func NewServiceWithGames(g []domaingames.Game) *games.Service {
	ms := store.NewMemoryStore()
	if len(g) > 0 {
		ms.SetGames(g)
	}
	return games.NewService(ms)
}

// NewServices builds games, teams, and players services sharing a single in-memory store.
func NewServices(g []domaingames.Game, t []domainteams.Team, p []domainplayers.Player) (*games.Service, *appteams.Service, *appplayers.Service) {
	ms := store.NewMemoryStore()
	if len(g) > 0 {
		ms.SetGames(g)
	}
	if len(t) > 0 {
		ms.SetTeams(t)
	}
	if len(p) > 0 {
		ms.SetPlayers(p)
	}
	return games.NewService(ms), appteams.NewService(ms), appplayers.NewService(ms)
}
