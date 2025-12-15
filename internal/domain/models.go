package domain

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

// Team represents the normalized team shape.
type Team struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	ExternalID int    `json:"externalId"`
}

// GameMeta stores provider metadata for a game.
type GameMeta struct {
	Season         string `json:"season"`
	UpstreamGameID int    `json:"upstreamGameId"`
}

// Game is the canonical game shape exposed by the service.
type Game struct {
	ID        string     `json:"id"`
	Provider  string     `json:"provider"`
	HomeTeam  Team       `json:"homeTeam"`
	AwayTeam  Team       `json:"awayTeam"`
	StartTime string     `json:"startTime"`
	Status    GameStatus `json:"status"`
	Score     Score      `json:"score"`
	Meta      GameMeta   `json:"meta"`
}

// TodayResponse is the payload returned by /games/today.
type TodayResponse struct {
	Date  string `json:"date"`
	Games []Game `json:"games"`
}
