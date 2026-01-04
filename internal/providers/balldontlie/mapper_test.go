package balldontlie

import (
	"testing"

	"github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain/players"
	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
)

func TestMapGameTransformsFields(t *testing.T) {
	resp := gameResponse{
		ID:               42,
		Date:             "2024-01-02T20:00:00Z",
		Status:           "In Progress",
		Time:             "Q3 05:00",
		Period:           3,
		Postseason:       true,
		HomeTeam:         teamResponse{ID: 10, FullName: "Home Squad", Abbreviation: "HMS", City: "Home", Conference: "East", Division: "Atlantic"},
		VisitorTeam:      teamResponse{ID: 20, FullName: "Away Squad", Abbreviation: "AWS", City: "Away", Conference: "West", Division: "Pacific"},
		HomeTeamScore:    55,
		VisitorTeamScore: 50,
		Season:           2024,
	}

	game := mapGame(resp)

	if game.ID != "balldontlie-42" || game.Provider != "balldontlie" {
		t.Fatalf("unexpected id/provider: %+v", game)
	}
	if game.Status != games.StatusInProgress {
		t.Fatalf("expected in progress status, got %s", game.Status)
	}
	if game.Score.Home != 55 || game.Score.Away != 50 {
		t.Fatalf("unexpected scores %+v", game.Score)
	}
	if game.Meta.UpstreamGameID != 42 || game.Meta.Season != "2024" {
		t.Fatalf("unexpected meta %+v", game.Meta)
	}
	if game.Meta.Period != 3 || !game.Meta.Postseason || game.Meta.Time != "Q3 05:00" {
		t.Fatalf("unexpected meta extras %+v", game.Meta)
	}
	if game.HomeTeam.ID != "team-10" || game.AwayTeam.ID != "team-20" {
		t.Fatalf("unexpected team ids home=%s away=%s", game.HomeTeam.ID, game.AwayTeam.ID)
	}
	if game.HomeTeam.Abbreviation != "HMS" || game.HomeTeam.City != "Home" || game.HomeTeam.Conference != "East" || game.HomeTeam.Division != "Atlantic" {
		t.Fatalf("unexpected home team extras %+v", game.HomeTeam)
	}
}

func TestMapStatusCoversVariants(t *testing.T) {
	cases := map[string]games.GameStatus{
		"Final":       games.StatusFinal,
		"In Progress": games.StatusInProgress,
		"Postponed":   games.StatusPostponed,
		"Canceled":    games.StatusCanceled,
		"Unknown":     games.StatusScheduled,
	}

	for input, expected := range cases {
		if got := mapStatus(input); got != expected {
			t.Fatalf("status %s expected %s, got %s", input, expected, got)
		}
	}
}

func TestFormatSeason(t *testing.T) {
	if got := formatSeason(2024); got != "2024" {
		t.Fatalf("expected season to be formatted as string, got %s", got)
	}
}

func TestMapTeamCoversFields(t *testing.T) {
	raw := teamResponse{
		ID:           9,
		Abbreviation: "ABC",
		City:         "City",
		Conference:   "East",
		Division:     "Atlantic",
		FullName:     "ABC City",
		Name:         "ABC",
	}

	team := mapTeam(raw)
	expected := teams.Team{
		ID:           "team-9",
		Name:         "ABC",
		FullName:     "ABC City",
		Abbreviation: "ABC",
		City:         "City",
		Conference:   "East",
		Division:     "Atlantic",
	}
	if team != expected {
		t.Fatalf("unexpected team %+v", team)
	}
}

func TestMapPlayerCoversFields(t *testing.T) {
	raw := playerResponse{
		ID:           12,
		FirstName:    "Jane",
		LastName:     "Doe",
		Position:     "G",
		HeightFeet:   6,
		HeightInches: 1,
		WeightPounds: 190,
		Team: teamResponse{
			ID:           9,
			Abbreviation: "ABC",
			City:         "City",
			Conference:   "East",
			Division:     "Atlantic",
			FullName:     "ABC City",
			Name:         "ABC",
		},
		College:      "College",
		Country:      "USA",
		JerseyNumber: "7",
	}

	player := mapPlayer(raw)
	if player.ID != "player-12" || player.FirstName != "Jane" || player.LastName != "Doe" || player.Position != "G" {
		t.Fatalf("unexpected base fields %+v", player)
	}
	if player.HeightFeet != 6 || player.HeightInches != 1 || player.WeightPounds != 190 {
		t.Fatalf("unexpected measurements %+v", player)
	}
	if player.Team.ID != "team-9" || player.Team.FullName != "ABC City" {
		t.Fatalf("unexpected team mapping %+v", player.Team)
	}
	expectedMeta := players.PlayerMeta{
		UpstreamPlayerID: 12,
		College:          "College",
		Country:          "USA",
		JerseyNumber:     "7",
	}
	if player.Meta != expectedMeta {
		t.Fatalf("unexpected meta %+v", player.Meta)
	}
}
