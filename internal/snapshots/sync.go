package snapshots

import (
	"context"
	"log/slog"
	"os"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/logging"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
	"github.com/preston-bernstein/nba-data-service/internal/timeutil"
)

// Syncer backfills and prunes game snapshots on a schedule.
type Syncer struct {
	provider  providers.GameProvider
	writer    *Writer
	cfg       SyncConfig
	logger    *slog.Logger
	now       func() time.Time
	loc       *time.Location
	newTicker func(time.Duration) *time.Ticker
}

// SyncConfig controls snapshot sync behavior.
type SyncConfig struct {
	Enabled      bool
	Days         int
	FutureDays   int
	Interval     time.Duration
	DailyHourUTC int
}

// NewSyncer constructs a snapshot syncer for games.
func NewSyncer(provider providers.GameProvider, writer *Writer, cfg SyncConfig, logger *slog.Logger, loc *time.Location) *Syncer {
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
	if loc == nil {
		loc = time.UTC
	}

	return &Syncer{
		provider:  provider,
		writer:    writer,
		cfg:       cfg,
		logger:    logger,
		now:       time.Now,
		loc:       loc,
		newTicker: time.NewTicker,
	}
}

// Run performs a backfill and schedules daily refreshes. Call in a goroutine.
func (s *Syncer) Run(ctx context.Context) {
	if s == nil || !s.cfg.Enabled || s.writer == nil || s.provider == nil {
		return
	}

	logging.Info(
		s.logger,
		"snapshot sync starting",
		"past_days", s.cfg.Days,
		"future_days", s.cfg.FutureDays,
		"interval", s.cfg.Interval.String(),
		"daily_hour_utc", s.cfg.DailyHourUTC,
	)

	now := s.now().In(s.loc)
	s.backfill(ctx, now)
	if s.logger != nil {
		s.logger.Info("snapshot sync initial backfill complete",
			"msg", "today, yesterday, and 2 days ago refreshed; stale data for those dates should be gone",
		)
	}
	go s.daily(ctx)
}

func (s *Syncer) backfill(ctx context.Context, now time.Time) {
	// Delete all snapshot files so we re-fetch the full window and never serve stale data.
	if s.writer != nil {
		if err := s.writer.DeleteAllGamesSnapshots(); err != nil {
			logging.Warn(s.logger, "snapshot wipe failed", "err", err)
		}
	}
	dates := s.buildDates(now)
	if s.logger != nil && len(dates) > 0 {
		s.logger.Info("snapshot backfill dates", "count", len(dates), "first_dates", dates[:min(3, len(dates))])
	}
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
				s.backfill(ctx, s.now().In(s.loc))
			}
		}
	}
}

// alwaysRefreshPastDays is how many past days (including today) we always re-fetch
// so that historical snapshots get final scores instead of staying SCHEDULED/0-0.
const alwaysRefreshPastDays = 3

func (s *Syncer) buildDates(now time.Time) []string {
	var dates []string

	// Always refresh today and the last (alwaysRefreshPastDays - 1) days so that
	// when users look backwards they see final scores, not stale SCHEDULED snapshots.
	for i := 0; i < alwaysRefreshPastDays && i < s.cfg.Days; i++ {
		dates = append(dates, timeutil.FormatDate(now.AddDate(0, 0, -i)))
	}

	// Past window beyond that: only fetch if missing (startup/outage).
	for i := alwaysRefreshPastDays; i < s.cfg.Days; i++ {
		date := timeutil.FormatDate(now.AddDate(0, 0, -i))
		if !s.hasSnapshot(date) {
			dates = append(dates, date)
		}
	}

	// Future window: prefetch missing only.
	for i := 1; i <= s.cfg.FutureDays; i++ {
		date := timeutil.FormatDate(now.AddDate(0, 0, i))
		if !s.hasSnapshot(date) {
			dates = append(dates, date)
		}
	}

	return dates
}

func (s *Syncer) fetchAndWrite(ctx context.Context, date string) {
	start := time.Now()
	games, err := s.provider.FetchGames(ctx, date, "")
	if err != nil {
		logging.Warn(s.logger, "snapshot sync fetch failed", "date", date, "err", err)
		return
	}
	if len(games) == 0 {
		logging.Warn(s.logger, "snapshot sync received no games", "date", date)
		return
	}
	snap := domaingames.NewTodayResponse(date, games)
	if err := s.writer.WriteGamesSnapshot(date, snap); err != nil {
		logging.Warn(s.logger, "snapshot sync write failed", "date", date, "err", err)
		return
	}
	logging.Info(s.logger, "snapshot written",
		"date", date,
		"count", len(games),
		"duration_ms", time.Since(start).Milliseconds(),
	)
}

func (s *Syncer) hasSnapshot(date string) bool {
	if s == nil || s.writer == nil || s.writer.basePath == "" || date == "" {
		return false
	}
	path := s.writer.snapshotPath(kindGames, date, 0)
	_, err := os.Stat(path)
	return err == nil
}

func (s *Syncer) sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
