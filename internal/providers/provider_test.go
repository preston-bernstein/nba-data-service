package providers

import (
	"context"
	"testing"

	"github.com/preston-bernstein/nba-data-service/internal/domain/games"
)

type testProvider struct{}

func (t *testProvider) FetchGames(ctx context.Context, date string, tz string) ([]games.Game, error) {
	_ = ctx
	_ = date
	_ = tz
	return nil, nil
}

func TestGameProviderInterfaceImplemented(t *testing.T) {
	var _ GameProvider = (*testProvider)(nil)
}
