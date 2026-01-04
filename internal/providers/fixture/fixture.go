package fixture

import (
	"context"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain/players"
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

// FetchTeams returns a deterministic set of teams.
func (p *Provider) FetchTeams(ctx context.Context) ([]teams.Team, error) {
	_ = ctx
	return []teams.Team{
		{ID: "bos", Name: "Celtics", FullName: "Boston Celtics", Abbreviation: "BOS", City: "Boston", Conference: "East", Division: "Atlantic"},
		{ID: "lal", Name: "Lakers", FullName: "Los Angeles Lakers", Abbreviation: "LAL", City: "Los Angeles", Conference: "West", Division: "Pacific"},
		{ID: "gsw", Name: "Warriors", FullName: "Golden State Warriors", Abbreviation: "GSW", City: "San Francisco", Conference: "West", Division: "Pacific"},
		{ID: "mia", Name: "Heat", FullName: "Miami Heat", Abbreviation: "MIA", City: "Miami", Conference: "East", Division: "Southeast"},
	}, nil
}

// FetchPlayers returns a deterministic set of players.
func (p *Provider) FetchPlayers(ctx context.Context) ([]players.Player, error) {
	_ = ctx
	return []players.Player{
		{
			ID:        "player-1",
			FirstName: "Jane",
			LastName:  "Doe",
			Position:  "G",
			Team:      teams.Team{ID: "bos", Name: "Celtics", Abbreviation: "BOS", City: "Boston", Conference: "East", Division: "Atlantic"},
			Meta: players.PlayerMeta{
				UpstreamPlayerID: 101,
				College:          "College A",
				Country:          "USA",
				JerseyNumber:     "1",
			},
		},
		{
			ID:        "player-2",
			FirstName: "John",
			LastName:  "Smith",
			Position:  "F",
			Team:      teams.Team{ID: "lal", Name: "Lakers", Abbreviation: "LAL", City: "Los Angeles", Conference: "West", Division: "Pacific"},
			Meta: players.PlayerMeta{
				UpstreamPlayerID: 102,
				College:          "College B",
				Country:          "USA",
				JerseyNumber:     "23",
			},
		},
	}, nil
}
