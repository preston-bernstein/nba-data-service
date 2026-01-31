package balldontlie

import (
	"fmt"
	"strings"

	"github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/domain/teams"
)

func mapGame(g gameResponse) games.Game {
	status := strings.TrimSpace(g.Status)

	return games.Game{
		ID:         fmt.Sprintf("%s-%d", providerName, g.ID),
		Provider:   providerName,
		HomeTeam:   mapTeam(g.HomeTeam),
		AwayTeam:   mapTeam(g.VisitorTeam),
		StartTime:  g.Datetime,
		Status:     status,
		StatusKind: mapStatusKind(status),
		Score: games.Score{
			Home: g.HomeTeamScore,
			Away: g.VisitorTeamScore,
		},
		Meta: games.GameMeta{
			Season:         formatSeason(g.Season),
			UpstreamGameID: g.ID,
			Period:         g.Period,
			Postseason:     g.Postseason,
			Time:           strings.TrimSpace(g.Time),
		},
	}
}

func mapTeam(t teamResponse) teams.Team {
	return teams.Team{
		ID:           t.Abbreviation,
		Name:         t.Name,
		FullName:     t.FullName,
		Abbreviation: t.Abbreviation,
		City:         t.City,
		Conference:   t.Conference,
		Division:     t.Division,
	}
}

func mapStatusKind(status string) games.GameStatusKind {
	switch strings.ToLower(status) {
	case "final", "ended":
		return games.StatusFinal
	case "in progress", "halftime", "end of period":
		return games.StatusInProgress
	case "postponed":
		return games.StatusPostponed
	case "canceled", "cancelled":
		return games.StatusCanceled
	default:
		return games.StatusScheduled
	}
}

func formatSeason(season int) string {
	return fmt.Sprintf("%d", season)
}
