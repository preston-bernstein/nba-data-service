package balldontlie

import (
	"context"

	"nba-games-service/internal/domain"
)

// Client is a placeholder for the balldontlie provider integration.
// Implement fetching and mapping once provider credentials and contracts are finalized.
type Client struct{}

func NewClient() *Client {
	return &Client{}
}

// FetchGames currently returns no games and no error until implemented.
func (c *Client) FetchGames(ctx context.Context) ([]domain.Game, error) {
	_ = ctx
	return nil, nil
}
