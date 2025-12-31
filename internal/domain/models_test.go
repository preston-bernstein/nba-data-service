package domain

import (
	"reflect"
	"testing"
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
