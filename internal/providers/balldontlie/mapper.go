package balldontlie

import (
	"fmt"
	"strings"

	"github.com/preston-bernstein/nba-data-service/internal/domain"
)

func mapGame(g gameResponse) domain.Game {
	return domain.Game{
		ID:        fmt.Sprintf("%s-%d", providerName, g.ID),
		Provider:  providerName,
		HomeTeam:  mapTeam(g.HomeTeam),
		AwayTeam:  mapTeam(g.VisitorTeam),
		StartTime: g.Date,
		Status:    mapStatus(g.Status),
		Score: domain.Score{
			Home: g.HomeTeamScore,
			Away: g.VisitorTeamScore,
		},
		Meta: domain.GameMeta{
			Season:         formatSeason(g.Season),
			UpstreamGameID: g.ID,
			Period:         g.Period,
			Postseason:     g.Postseason,
			Time:           strings.TrimSpace(g.Time),
		},
	}
}

func mapTeam(t teamResponse) domain.Team {
	return domain.Team{
		ID:           fmt.Sprintf("team-%d", t.ID),
		Name:         t.FullName,
		ExternalID:   t.ID,
		Abbreviation: t.Abbreviation,
		City:         t.City,
		Conference:   t.Conference,
		Division:     t.Division,
	}
}

func mapStatus(status string) domain.GameStatus {
	switch strings.ToLower(status) {
	case "final", "ended":
		return domain.StatusFinal
	case "in progress", "halftime", "end of period":
		return domain.StatusInProgress
	case "postponed":
		return domain.StatusPostponed
	case "canceled", "cancelled":
		return domain.StatusCanceled
	default:
		return domain.StatusScheduled
	}
}

func formatSeason(season int) string {
	return fmt.Sprintf("%d", season)
}
