package games

import "github.com/preston-bernstein/nba-data-service/internal/domain/teams"

// GameStatusKind normalizes provider status into a small enum.
type GameStatusKind string

const (
	StatusScheduled  GameStatusKind = "SCHEDULED"
	StatusInProgress GameStatusKind = "IN_PROGRESS"
	StatusFinal      GameStatusKind = "FINAL"
	StatusPostponed  GameStatusKind = "POSTPONED"
	StatusCanceled   GameStatusKind = "CANCELED"
)

// Score captures home and away points.
type Score struct {
	Home int `json:"home"`
	Away int `json:"away"`
}

// GameMeta stores provider metadata for a game.
type GameMeta struct {
	Season         string `json:"season"`
	UpstreamGameID int    `json:"upstreamGameId"`
	Period         int    `json:"period,omitempty"`
	Postseason     bool   `json:"postseason,omitempty"`
	Time           string `json:"time,omitempty"`
}

// Game is the canonical game shape exposed by the service.
type Game struct {
	ID         string         `json:"id"`
	Provider   string         `json:"provider"`
	HomeTeam   teams.Team     `json:"homeTeam"`
	AwayTeam   teams.Team     `json:"awayTeam"`
	StartTime  string         `json:"startTime"`
	Status     string         `json:"status"`
	StatusKind GameStatusKind `json:"statusKind"`
	Score      Score          `json:"score"`
	Meta       GameMeta       `json:"meta"`
}

// TodayResponse is the payload returned by /games?date=YYYY-MM-DD.
type TodayResponse struct {
	Date  string `json:"date"`
	Games []Game `json:"games"`
}

// NewTodayResponse builds a TodayResponse payload.
func NewTodayResponse(date string, games []Game) TodayResponse {
	return TodayResponse{
		Date:  date,
		Games: games,
	}
}
