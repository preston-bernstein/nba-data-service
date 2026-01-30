package games

import "github.com/preston-bernstein/nba-data-service/internal/domain/teams"

// GameStatus mirrors the shared contract for game lifecycle states.
type GameStatus string

const (
	StatusScheduled  GameStatus = "SCHEDULED"
	StatusInProgress GameStatus = "IN_PROGRESS"
	StatusFinal      GameStatus = "FINAL"
	StatusPostponed  GameStatus = "POSTPONED"
	StatusCanceled   GameStatus = "CANCELED"
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
	ID        string     `json:"id"`
	Provider  string     `json:"provider"`
	HomeTeam  teams.Team `json:"homeTeam"`
	AwayTeam  teams.Team `json:"awayTeam"`
	StartTime string     `json:"startTime"`
	Status    GameStatus `json:"status"`
	Score     Score      `json:"score"`
	Meta      GameMeta   `json:"meta"`
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
