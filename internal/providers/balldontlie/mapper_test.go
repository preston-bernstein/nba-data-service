package balldontlie

import (
	"testing"

	"nba-games-service/internal/domain"
)

func TestMapGameTransformsFields(t *testing.T) {
	resp := gameResponse{
		ID:               42,
		Date:             "2024-01-02T20:00:00Z",
		Status:           "In Progress",
		HomeTeam:         teamResponse{ID: 10, FullName: "Home Squad"},
		VisitorTeam:      teamResponse{ID: 20, FullName: "Away Squad"},
		HomeTeamScore:    55,
		VisitorTeamScore: 50,
		Season:           2024,
	}

	game := mapGame(resp)

	if game.ID != "balldontlie-42" || game.Provider != "balldontlie" {
		t.Fatalf("unexpected id/provider: %+v", game)
	}
	if game.Status != domain.StatusInProgress {
		t.Fatalf("expected in progress status, got %s", game.Status)
	}
	if game.Score.Home != 55 || game.Score.Away != 50 {
		t.Fatalf("unexpected scores %+v", game.Score)
	}
	if game.Meta.UpstreamGameID != 42 || game.Meta.Season != "2024" {
		t.Fatalf("unexpected meta %+v", game.Meta)
	}
	if game.HomeTeam.ID != "team-10" || game.AwayTeam.ID != "team-20" {
		t.Fatalf("unexpected team ids home=%s away=%s", game.HomeTeam.ID, game.AwayTeam.ID)
	}
}

func TestMapStatusCoversVariants(t *testing.T) {
	cases := map[string]domain.GameStatus{
		"Final":       domain.StatusFinal,
		"In Progress": domain.StatusInProgress,
		"Postponed":   domain.StatusPostponed,
		"Canceled":    domain.StatusCanceled,
		"Unknown":     domain.StatusScheduled,
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
