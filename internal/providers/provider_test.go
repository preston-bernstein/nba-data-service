package providers

import (
	"context"
	"testing"

	"nba-data-service/internal/domain"
)

type testProvider struct{}

func (t *testProvider) FetchGames(ctx context.Context, date string, tz string) ([]domain.Game, error) {
	_ = ctx
	_ = date
	_ = tz
	return nil, nil
}

func TestGameProviderInterfaceImplemented(t *testing.T) {
	var _ GameProvider = (*testProvider)(nil)
}
