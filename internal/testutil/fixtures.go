package testutil

import (
	"github.com/preston-bernstein/nba-data-service/internal/domain"
)

// SampleGame returns a minimal game fixture with the provided id.
func SampleGame(id string) domain.Game {
	return domain.Game{
		ID:       id,
		Provider: "test",
		HomeTeam: domain.Team{ID: "home", Name: "Home", ExternalID: 1},
		AwayTeam: domain.Team{ID: "away", Name: "Away", ExternalID: 2},
		Status:   domain.StatusScheduled,
		Score:    domain.Score{Home: 0, Away: 0},
		Meta:     domain.GameMeta{Season: "2023-2024", UpstreamGameID: 1},
	}
}

// SampleTodayResponse builds a TodayResponse with a single sample game and date.
func SampleTodayResponse(date string, id string) domain.TodayResponse {
	return domain.TodayResponse{
		Date:  date,
		Games: []domain.Game{SampleGame(id)},
	}
}
