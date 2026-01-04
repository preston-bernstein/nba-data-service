package players

import (
	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
)

// Player represents the normalized player shape (balldontlie-aligned).
type Player struct {
	ID           string     `json:"id"`
	FirstName    string     `json:"firstName"`
	LastName     string     `json:"lastName"`
	Position     string     `json:"position"`
	HeightFeet   int        `json:"heightFeet"`
	HeightInches int        `json:"heightInches"`
	WeightPounds int        `json:"weightPounds"`
	Team         teams.Team `json:"team"`
	Meta         PlayerMeta `json:"meta"`
}

// PlayerMeta holds upstream metadata.
type PlayerMeta struct {
	UpstreamPlayerID int    `json:"upstreamPlayerId"`
	College          string `json:"college"`
	Country          string `json:"country"`
	JerseyNumber     string `json:"jerseyNumber"`
}
