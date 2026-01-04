package balldontlie

const providerName = "balldontlie"

type gamesResponse struct {
	Data []gameResponse `json:"data"`
	Meta metaResponse   `json:"meta"`
}

type teamsResponse struct {
	Data []teamResponse `json:"data"`
	Meta metaResponse   `json:"meta"`
}

type playersResponse struct {
	Data []playerResponse `json:"data"`
	Meta metaResponse     `json:"meta"`
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

type playerResponse struct {
	ID           int          `json:"id"`
	FirstName    string       `json:"first_name"`
	LastName     string       `json:"last_name"`
	Position     string       `json:"position"`
	HeightFeet   int          `json:"height_feet"`
	HeightInches int          `json:"height_inches"`
	WeightPounds int          `json:"weight_pounds"`
	Team         teamResponse `json:"team"`
	College      string       `json:"college"`
	Country      string       `json:"country"`
	JerseyNumber string       `json:"jersey_number"`
}

type metaResponse struct {
	TotalPages int `json:"total_pages"`
}
