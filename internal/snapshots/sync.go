package snapshots

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	domainplayers "github.com/preston-bernstein/nba-data-service/internal/domain/players"
	domainteams "github.com/preston-bernstein/nba-data-service/internal/domain/teams"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
)

// RosterStore updates in-memory teams/players when static snapshots refresh.
type RosterStore interface {
	SetTeams([]domainteams.Team)
	SetPlayers([]domainplayers.Player)
}

// Syncer backfills and prunes snapshots on a schedule.
type Syncer struct {
	gameProvider   providers.GameProvider
	teamProvider   providers.TeamProvider
	playerProvider providers.PlayerProvider
	writer         *Writer
	cfg            SyncConfig
	logger         *slog.Logger
	now            func() time.Time
	rosterStore    RosterStore
	newTicker      func(time.Duration) *time.Ticker
}

// SyncConfig controls snapshot sync behavior.
type SyncConfig struct {
	Enabled             bool
	Days                int
	FutureDays          int
	Interval            time.Duration
	DailyHourUTC        int
	TeamsRefreshDays    int
	PlayersRefreshHours int
}

// NewSyncer constructs a snapshot syncer.
func NewSyncer(provider providers.DataProvider, writer *Writer, cfg SyncConfig, logger *slog.Logger, rosterStore RosterStore) *Syncer {
	if cfg.Days <= 0 {
		cfg.Days = 7
	}
	if cfg.FutureDays < 0 {
		cfg.FutureDays = 0
	}
	if cfg.Interval <= 0 {
		cfg.Interval = time.Minute
	}
	if cfg.DailyHourUTC < 0 || cfg.DailyHourUTC > 23 {
		cfg.DailyHourUTC = 2
	}
	if cfg.TeamsRefreshDays <= 0 {
		cfg.TeamsRefreshDays = 7
	}
	if cfg.PlayersRefreshHours <= 0 {
		cfg.PlayersRefreshHours = 24
	}

	return &Syncer{
		gameProvider:   provider,
		teamProvider:   provider,
		playerProvider: provider,
		writer:         writer,
		cfg:            cfg,
		logger:         logger,
		now:            time.Now,
		rosterStore:    rosterStore,
		newTicker:      time.NewTicker,
	}
}

// Run performs a one-time backfill for the last N days, spaced by Interval.
// Callers should run this in a goroutine.
func (s *Syncer) Run(ctx context.Context) {
	if s == nil || !s.cfg.Enabled || s.writer == nil {
		return
	}
	if s.gameProvider == nil && s.teamProvider == nil && s.playerProvider == nil {
		return
	}
	s.logInfo(
		"snapshot sync starting",
		"past_days", s.cfg.Days,
		"future_days", s.cfg.FutureDays,
		"interval", s.cfg.Interval.String(),
		"daily_hour_utc", s.cfg.DailyHourUTC,
		"teams_refresh_days", s.cfg.TeamsRefreshDays,
		"players_refresh_hours", s.cfg.PlayersRefreshHours,
	)
	s.syncStatic(ctx, s.now().UTC())
	s.backfill(ctx, s.now().UTC())
	go s.daily(ctx)
}

func (s *Syncer) backfill(ctx context.Context, now time.Time) {
	dates := s.buildDates(now)
	for i, date := range dates {
		select {
		case <-ctx.Done():
			return
		default:
		}
		s.fetchAndWrite(ctx, date)
		if i < len(dates)-1 {
			s.sleep(ctx, s.cfg.Interval)
		}
	}
}

func (s *Syncer) daily(ctx context.Context) {
	ticker := s.newTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if now.UTC().Hour() == s.cfg.DailyHourUTC {
				current := s.now().UTC()
				s.syncStatic(ctx, current)
				s.backfill(ctx, current)
			}
		}
	}
}

func (s *Syncer) buildDates(now time.Time) []string {
	var dates []string
	today := now.Format("2006-01-02")
	yesterday := now.AddDate(0, 0, -1).Format("2006-01-02")

	// Always refresh today and yesterday to capture live/final scores.
	dates = append(dates, today, yesterday)

	// Past window beyond yesterday: only fetch if missing (e.g., startup or outage).
	for i := 2; i < s.cfg.Days; i++ {
		date := now.AddDate(0, 0, -i).Format("2006-01-02")
		if !s.hasSnapshot(kindGames, date) {
			dates = append(dates, date)
		}
	}

	// Future window: prefetch missing only.
	for i := 1; i <= s.cfg.FutureDays; i++ {
		date := now.AddDate(0, 0, i).Format("2006-01-02")
		if !s.hasSnapshot(kindGames, date) {
			dates = append(dates, date)
		}
	}

	return dates
}

func (s *Syncer) fetchAndWrite(ctx context.Context, date string) {
	start := time.Now()
	if s.gameProvider == nil {
		s.logWarn("snapshot sync fetch failed", "date", date, "err", "game provider unavailable")
		return
	}
	games, err := s.gameProvider.FetchGames(ctx, date, "")
	if err != nil {
		s.logWarn("snapshot sync fetch failed", "date", date, "err", err)
		return
	}
	if len(games) == 0 {
		s.logWarn("snapshot sync received no games", "date", date)
		return
	}
	snap := domaingames.TodayResponse{
		Date:  date,
		Games: games,
	}
	if err := s.writer.WriteGamesSnapshot(date, snap); err != nil {
		s.logWarn("snapshot sync write failed", "date", date, "err", err)
		return
	}
	s.logInfo("snapshot written",
		"date", date,
		"count", len(games),
		"duration_ms", time.Since(start).Milliseconds(),
	)
}

func (s *Syncer) syncStatic(ctx context.Context, now time.Time) {
	date := now.Format("2006-01-02")
	s.syncTeams(ctx, now, date)
	s.syncPlayers(ctx, now, date)
}

func (s *Syncer) syncTeams(ctx context.Context, now time.Time, date string) {
	if s.teamProvider == nil {
		return
	}
	if !s.shouldRefresh(kindTeams, now) {
		return
	}
	start := time.Now()
	items, err := s.teamProvider.FetchTeams(ctx)
	if err != nil {
		s.logWarn("teams snapshot fetch failed", "err", err)
		return
	}
	if err := s.writer.WriteTeamsSnapshot(date, TeamsSnapshot{Date: date, Teams: items}); err != nil {
		s.logWarn("teams snapshot write failed", "err", err)
		return
	}
	if s.rosterStore != nil {
		s.rosterStore.SetTeams(items)
	}
	s.logInfo("teams snapshot written", "count", len(items), "duration_ms", time.Since(start).Milliseconds())
}

func (s *Syncer) syncPlayers(ctx context.Context, now time.Time, date string) {
	if s.playerProvider == nil {
		return
	}
	if !s.shouldRefresh(kindPlayers, now) {
		return
	}
	start := time.Now()
	items, err := s.playerProvider.FetchPlayers(ctx)
	if err != nil {
		s.logWarn("players snapshot fetch failed", "err", err)
		return
	}
	if err := s.writer.WritePlayersSnapshot(date, PlayersSnapshot{Date: date, Players: items}); err != nil {
		s.logWarn("players snapshot write failed", "err", err)
		return
	}
	if s.rosterStore != nil {
		s.rosterStore.SetPlayers(items)
	}
	s.logInfo("players snapshot written", "count", len(items), "duration_ms", time.Since(start).Milliseconds())
}

func (s *Syncer) sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func (s *Syncer) logInfo(msg string, args ...any) {
	if s.logger != nil {
		s.logger.Info(msg, args...)
	}
}

func (s *Syncer) logWarn(msg string, args ...any) {
	if s.logger != nil {
		s.logger.Warn(msg, args...)
	}
}

func (s *Syncer) hasSnapshot(kind snapshotKind, date string) bool {
	if s == nil || s.writer == nil || s.writer.basePath == "" || date == "" {
		return false
	}
	path := s.writer.snapshotPath(kind, date)
	_, err := os.Stat(path)
	return err == nil
}

func (s *Syncer) shouldRefresh(kind snapshotKind, now time.Time) bool {
	if s == nil || s.writer == nil {
		return true
	}
	manifestPath := filepath.Join(s.writer.basePath, "manifest.json")
	m, _ := readManifest(manifestPath, s.writer.retentionDays)

	switch kind {
	case kindTeams:
		if m.Teams.LastRefreshed.IsZero() {
			return true
		}
		next := m.Teams.LastRefreshed.AddDate(0, 0, s.cfg.TeamsRefreshDays)
		return !now.Before(next)
	case kindPlayers:
		if m.Players.LastRefreshed.IsZero() {
			return true
		}
		next := m.Players.LastRefreshed.Add(time.Duration(s.cfg.PlayersRefreshHours) * time.Hour)
		return !now.Before(next)
	default:
		return true
	}
}
