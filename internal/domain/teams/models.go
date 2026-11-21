package teams

// Team is the normalized team shape used inside games.
// Kept in its own package to keep domain models modular and reusable across providers/fixtures.
type Team struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	FullName     string `json:"fullName"`
	Abbreviation string `json:"abbreviation"`
	City         string `json:"city"`
	Conference   string `json:"conference"`
	Division     string `json:"division"`
}
