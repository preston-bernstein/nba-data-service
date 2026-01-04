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
	Time             string       `json:"time"`
	Period           int          `json:"period"`
	Postseason       bool         `json:"postseason"`
	HomeTeam         teamResponse `json:"home_team"`
	VisitorTeam      teamResponse `json:"visitor_team"`
	HomeTeamScore    int          `json:"home_team_score"`
	VisitorTeamScore int          `json:"visitor_team_score"`
	Season           int          `json:"season"`
}

type teamResponse struct {
	ID           int    `json:"id"`
	Abbreviation string `json:"abbreviation"`
	City         string `json:"city"`
	Conference   string `json:"conference"`
	Division     string `json:"division"`
	FullName     string `json:"full_name"`
	Name         string `json:"name"`
}

type metaResponse struct {
	TotalPages int `json:"total_pages"`
}
