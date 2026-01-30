package games

import (
	"reflect"
	"testing"

	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
)

func TestGameStatusValues(t *testing.T) {
	expected := map[GameStatus]string{
		StatusScheduled:  "SCHEDULED",
		StatusInProgress: "IN_PROGRESS",
		StatusFinal:      "FINAL",
		StatusPostponed:  "POSTPONED",
		StatusCanceled:   "CANCELED",
	}

	for status, want := range expected {
		if string(status) != want {
			t.Fatalf("expected %q got %q", want, status)
		}
	}
}

func TestGameJSONTags(t *testing.T) {
	type fieldCheck struct {
		name string
		tag  string
	}

	gameType := reflect.TypeOf(Game{})
	fields := []fieldCheck{
		{"ID", "id"},
		{"Provider", "provider"},
		{"HomeTeam", "homeTeam"},
		{"AwayTeam", "awayTeam"},
		{"StartTime", "startTime"},
		{"Status", "status"},
		{"Score", "score"},
		{"Meta", "meta"},
	}

	for _, fc := range fields {
		field, ok := gameType.FieldByName(fc.name)
		if !ok {
			t.Fatalf("missing field %s", fc.name)
		}
		if jsonTag := field.Tag.Get("json"); jsonTag != fc.tag {
			t.Fatalf("field %s expected json tag %s, got %s", fc.name, fc.tag, jsonTag)
		}
	}
}

func TestGameUsesTeamsDomain(t *testing.T) {
	g := Game{
		HomeTeam: teams.Team{ID: "t1", Name: "Home"},
		AwayTeam: teams.Team{ID: "t2", Name: "Away"},
	}
	if g.HomeTeam.Name != "Home" || g.AwayTeam.Name != "Away" {
		t.Fatalf("expected teams embedded from teams domain")
	}
}

func TestNewTodayResponse(t *testing.T) {
	date := "2024-01-15"
	games := []Game{
		{ID: "g1", Status: StatusScheduled},
		{ID: "g2", Status: StatusFinal},
	}

	resp := NewTodayResponse(date, games)

	if resp.Date != date {
		t.Fatalf("expected date %s, got %s", date, resp.Date)
	}
	if len(resp.Games) != 2 {
		t.Fatalf("expected 2 games, got %d", len(resp.Games))
	}
	if resp.Games[0].ID != "g1" || resp.Games[1].ID != "g2" {
		t.Fatalf("expected games in order, got %v", resp.Games)
	}
}
