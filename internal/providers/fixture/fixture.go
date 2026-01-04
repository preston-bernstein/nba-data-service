package fixture

import (
	"context"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
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
func (p *Provider) FetchGames(ctx context.Context, date string, tz string) ([]domaingames.Game, error) {
	_ = ctx
	_ = tz

	start := p.now().UTC().Truncate(time.Hour)
	if date != "" {
		parsed, err := time.Parse("2006-01-02", date)
		if err == nil {
			start = parsed.UTC()
		}
	}

	games := []domaingames.Game{
		{
			ID:        "fixture-1",
			Provider:  "fixture",
			HomeTeam:  teams.Team{ID: "bos", Name: "Celtics", Abbreviation: "BOS", City: "Boston", Conference: "East", Division: "Atlantic"},
			AwayTeam:  teams.Team{ID: "lal", Name: "Lakers", Abbreviation: "LAL", City: "Los Angeles", Conference: "West", Division: "Pacific"},
			StartTime: start.Add(2 * time.Hour).Format(time.RFC3339),
			Status:    domaingames.StatusScheduled,
			Score:     domaingames.Score{Home: 0, Away: 0},
			Meta:      domaingames.GameMeta{Season: "2023-2024", UpstreamGameID: 1001},
		},
		{
			ID:        "fixture-2",
			Provider:  "fixture",
			HomeTeam:  teams.Team{ID: "gsw", Name: "Warriors", Abbreviation: "GSW", City: "San Francisco", Conference: "West", Division: "Pacific"},
			AwayTeam:  teams.Team{ID: "mia", Name: "Heat", Abbreviation: "MIA", City: "Miami", Conference: "East", Division: "Southeast"},
			StartTime: start.Add(4 * time.Hour).Format(time.RFC3339),
			Status:    domaingames.StatusScheduled,
			Score:     domaingames.Score{Home: 0, Away: 0},
			Meta:      domaingames.GameMeta{Season: "2023-2024", UpstreamGameID: 1002},
		},
	}

	return games, nil
}
