package events

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
)

func sampleGame() domaingames.Game {
	return domaingames.Game{
		ID:         "g-1",
		Provider:   "fixture",
		HomeTeam:   teams.Team{ID: "1", Name: "Home"},
		AwayTeam:   teams.Team{ID: "2", Name: "Away"},
		StartTime:  "2024-01-15T00:00:00Z",
		Status:     "Q1",
		StatusKind: domaingames.StatusInProgress,
		Score:      domaingames.Score{Home: 10, Away: 8},
		Meta:       domaingames.GameMeta{Season: "2023-2024", UpstreamGameID: 7},
	}
}

func compileSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	raw, err := os.ReadFile("testdata/game.updated.v1.json")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	if err := c.AddResource("game.updated.v1.json", doc); err != nil {
		t.Fatal(err)
	}
	s, err := c.Compile("game.updated.v1.json")
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestEventMatchesSchema(t *testing.T) {
	schema := compileSchema(t)
	for _, kind := range []ChangeKind{ChangeCreated, ChangeStatus, ChangeScore} {
		ev := NewEvent(sampleGame(), kind, time.Date(2024, 1, 15, 1, 2, 3, 0, time.UTC))
		raw, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		inst, err := jsonschema.UnmarshalJSON(strings.NewReader(string(raw)))
		if err != nil {
			t.Fatal(err)
		}
		if err := schema.Validate(inst); err != nil {
			t.Fatalf("kind %s: %v\n%s", kind, err, raw)
		}
	}
}

func TestSchemaRejectsBadEvent(t *testing.T) {
	schema := compileSchema(t)
	inst, _ := jsonschema.UnmarshalJSON(strings.NewReader(`{"eventId":"not-a-uuid"}`))
	if err := schema.Validate(inst); err == nil {
		t.Fatal("expected schema to reject a bad event")
	}
}

func TestNewEventFields(t *testing.T) {
	at := time.Date(2024, 1, 15, 1, 2, 3, 0, time.UTC)
	a := NewEvent(sampleGame(), ChangeScore, at)
	b := NewEvent(sampleGame(), ChangeScore, at)
	if a.EventID == "" || a.EventID == b.EventID {
		t.Fatalf("event ids must be unique and set: %q %q", a.EventID, b.EventID)
	}
	if a.EventType != "game.updated" || a.SchemaVersion != 1 {
		t.Fatalf("bad type/version: %+v", a)
	}
	if a.OccurredAt != "2024-01-15T01:02:03Z" {
		t.Fatalf("bad occurredAt %q", a.OccurredAt)
	}
	if a.Key() != "g-1" {
		t.Fatalf("key should be game id, got %q", a.Key())
	}
}
