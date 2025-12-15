package providers

import (
	"context"
	"testing"

	"nba-games-service/internal/domain"
)

type testProvider struct{}

func (t *testProvider) FetchGames(ctx context.Context) ([]domain.Game, error) {
	return nil, nil
}

func TestGameProviderInterfaceImplemented(t *testing.T) {
	var _ GameProvider = (*testProvider)(nil)
}
