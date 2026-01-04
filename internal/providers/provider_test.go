package providers

import (
	"context"
	"testing"

	"github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain/players"
	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
)

type testProvider struct{}

func (t *testProvider) FetchGames(ctx context.Context, date string, tz string) ([]games.Game, error) {
	_ = ctx
	_ = date
	_ = tz
	return nil, nil
}

func (t *testProvider) FetchTeams(ctx context.Context) ([]teams.Team, error) {
	_ = ctx
	return nil, nil
}

func (t *testProvider) FetchPlayers(ctx context.Context) ([]players.Player, error) {
	_ = ctx
	return nil, nil
}

func TestGameProviderInterfaceImplemented(t *testing.T) {
	var _ GameProvider = (*testProvider)(nil)
}

func TestDataProviderInterfaceImplemented(t *testing.T) {
	var _ DataProvider = (*testProvider)(nil)
}
