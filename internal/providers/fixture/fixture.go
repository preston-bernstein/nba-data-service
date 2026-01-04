package fixture

import (
	"context"
	"time"

	"github.com/preston-bernstein/nba-data-service/internal/domain"
)

// Provider returns a static set of games useful for local testing and bootstrapping.
type Provider struct {
	now func() time.Time
}

// New creates a fixture provider with a time source.
func New() *Provider {
	return &Provider{
		now: time.Now,
	}
}

// FetchGames returns a deterministic set of example games.
func (p *Provider) FetchGames(ctx context.Context, date string, tz string) ([]domain.Game, error) {
	_ = ctx
	_ = tz

	start := p.now().UTC().Truncate(time.Hour)
	if date != "" {
		parsed, err := time.Parse("2006-01-02", date)
		if err == nil {
			start = parsed.UTC()
		}
	}

	games := []domain.Game{
		{
			ID:        "fixture-1",
			Provider:  "fixture",
			HomeTeam:  domain.Team{ID: "bos", Name: "Celtics", ExternalID: 1},
			AwayTeam:  domain.Team{ID: "lal", Name: "Lakers", ExternalID: 2},
			StartTime: start.Add(2 * time.Hour).Format(time.RFC3339),
			Status:    domain.StatusScheduled,
			Score:     domain.Score{Home: 0, Away: 0},
			Meta:      domain.GameMeta{Season: "2023-2024", UpstreamGameID: 1001},
		},
		{
			ID:        "fixture-2",
			Provider:  "fixture",
			HomeTeam:  domain.Team{ID: "gsw", Name: "Warriors", ExternalID: 3},
			AwayTeam:  domain.Team{ID: "mia", Name: "Heat", ExternalID: 4},
			StartTime: start.Add(4 * time.Hour).Format(time.RFC3339),
			Status:    domain.StatusScheduled,
			Score:     domain.Score{Home: 0, Away: 0},
			Meta:      domain.GameMeta{Season: "2023-2024", UpstreamGameID: 1002},
		},
	}

	return games, nil
}
