package testutil

import (
	"context"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
)

// GoodProvider returns the provided games with no error.
type GoodProvider struct {
	Games []domaingames.Game
}

func (p GoodProvider) FetchGames(ctx context.Context, date string, tz string) ([]domaingames.Game, error) {
	_ = ctx
	_ = date
	_ = tz
	return p.Games, nil
}

// ErrProvider always returns the provided error.
type ErrProvider struct {
	Err error
}

func (p ErrProvider) FetchGames(ctx context.Context, date string, tz string) ([]domaingames.Game, error) {
	return nil, p.Err
}

// EmptyProvider returns no games, no error.
type EmptyProvider struct{}

func (EmptyProvider) FetchGames(ctx context.Context, date string, tz string) ([]domaingames.Game, error) {
	return []domaingames.Game{}, nil
}

// UnavailableProvider returns ErrProviderUnavailable.
type UnavailableProvider struct{}

func (UnavailableProvider) FetchGames(ctx context.Context, date string, tz string) ([]domaingames.Game, error) {
	return nil, providers.ErrProviderUnavailable
}

// NotifyingProvider returns games and closes notify channel on first fetch.
type NotifyingProvider struct {
	Games  []domaingames.Game
	Notify chan struct{}
}

func (p *NotifyingProvider) FetchGames(ctx context.Context, date string, tz string) ([]domaingames.Game, error) {
	_ = ctx
	_ = date
	_ = tz
	if p.Notify != nil {
		select {
		case <-p.Notify:
		default:
			close(p.Notify)
		}
	}
	return p.Games, nil
}
