package teams

// Team represents the normalized team shape.
// Fields align with balldontlie responses.
type Team struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	FullName     string `json:"fullName"`
	Abbreviation string `json:"abbreviation"`
	City         string `json:"city"`
	Conference   string `json:"conference"`
	Division     string `json:"division"`
}
