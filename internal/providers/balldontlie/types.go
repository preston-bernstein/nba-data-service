package balldontlie

const providerName = "balldontlie"

type gamesResponse struct {
	Data []gameResponse `json:"data"`
	Meta metaResponse   `json:"meta"`
}

type gameResponse struct {
	ID               int          `json:"id"`
	Date             string       `json:"date"`
	Status           string       `json:"status"`
	HomeTeam         teamResponse `json:"home_team"`
	VisitorTeam      teamResponse `json:"visitor_team"`
	HomeTeamScore    int          `json:"home_team_score"`
	VisitorTeamScore int          `json:"visitor_team_score"`
	Season           int          `json:"season"`
}

type teamResponse struct {
	ID       int    `json:"id"`
	FullName string `json:"full_name"`
}

type metaResponse struct {
	TotalPages int `json:"total_pages"`
}
