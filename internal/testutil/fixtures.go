package testutil

import (
	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
)

// SampleGame returns a minimal game fixture with the provided id.
func SampleGame(id string) domaingames.Game {
	return domaingames.Game{
		ID:       id,
		Provider: "test",
		HomeTeam: teams.Team{ID: "home", Name: "Home"},
		AwayTeam: teams.Team{ID: "away", Name: "Away"},
		Status:   domaingames.StatusScheduled,
		Score:    domaingames.Score{Home: 0, Away: 0},
		Meta:     domaingames.GameMeta{Season: "2023-2024", UpstreamGameID: 1},
	}
}

// SampleTodayResponse builds a TodayResponse with a single sample game and date.
func SampleTodayResponse(date string, id string) domaingames.TodayResponse {
	return domaingames.NewTodayResponse(date, []domaingames.Game{SampleGame(id)})
}

// SampleTeam returns a minimal team fixture.
func SampleTeam(id string) teams.Team {
	return teams.Team{
		ID:           id,
		Name:         "Team " + id,
		FullName:     "Full Team " + id,
		Abbreviation: "T" + id,
		City:         "City",
		Conference:   "East",
		Division:     "Atlantic",
	}
}
