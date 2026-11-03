// Package events defines the game.updated event and how it is published.
package events

import (
	"time"

	"github.com/google/uuid"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
)

const (
	// TypeGameUpdated is the eventType value for game updates.
	TypeGameUpdated = "game.updated"
	// SchemaVersion is the version of the game.updated payload.
	SchemaVersion = 1
	// DefaultTopic is the Kafka topic for game.updated events.
	DefaultTopic = "game.updated"
)

// ChangeKind says why an event was sent.
type ChangeKind string

const (
	ChangeCreated ChangeKind = "created"
	ChangeStatus  ChangeKind = "status"
	ChangeScore   ChangeKind = "score"
)

// Event is the game.updated envelope. It matches testdata/game.updated.v1.json.
type Event struct {
	EventID       string           `json:"eventId"`
	EventType     string           `json:"eventType"`
	SchemaVersion int              `json:"schemaVersion"`
	OccurredAt    string           `json:"occurredAt"`
	ChangeKind    ChangeKind       `json:"changeKind"`
	Game          domaingames.Game `json:"game"`
}

// NewEvent builds an event with a fresh UUID. occurredAt is written in UTC.
func NewEvent(game domaingames.Game, kind ChangeKind, at time.Time) Event {
	return Event{
		EventID:       uuid.NewString(),
		EventType:     TypeGameUpdated,
		SchemaVersion: SchemaVersion,
		OccurredAt:    at.UTC().Format(time.RFC3339),
		ChangeKind:    kind,
		Game:          game,
	}
}

// Key is the Kafka message key. Events for one game share a partition.
func (e Event) Key() string {
	return e.Game.ID
}
