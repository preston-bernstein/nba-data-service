package snapshots

import (
	"context"
	"log/slog"
	"os"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
)

// Syncer backfills and prunes game snapshots on a schedule.
type Syncer struct {
	provider  providers.GameProvider
	writer    *Writer
	cfg       SyncConfig
	logger    *slog.Logger
	now       func() time.Time
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
func NewSyncer(provider providers.GameProvider, writer *Writer, cfg SyncConfig, logger *slog.Logger) *Syncer {
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

	return &Syncer{
		provider:  provider,
		writer:    writer,
		cfg:       cfg,
		logger:    logger,
		now:       time.Now,
		newTicker: time.NewTicker,
	}
}

// Run performs a backfill and schedules daily refreshes. Call in a goroutine.
func (s *Syncer) Run(ctx context.Context) {
	if s == nil || !s.cfg.Enabled || s.writer == nil || s.provider == nil {
		return
	}

	s.logInfo(
		"snapshot sync starting",
		"past_days", s.cfg.Days,
		"future_days", s.cfg.FutureDays,
		"interval", s.cfg.Interval.String(),
		"daily_hour_utc", s.cfg.DailyHourUTC,
	)

	now := s.now().UTC()
	s.backfill(ctx, now)
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
				s.backfill(ctx, s.now().UTC())
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

	// Past window beyond yesterday: only fetch if missing (startup/outage).
	for i := 2; i < s.cfg.Days; i++ {
		date := now.AddDate(0, 0, -i).Format("2006-01-02")
		if !s.hasSnapshot(date) {
			dates = append(dates, date)
		}
	}

	// Future window: prefetch missing only.
	for i := 1; i <= s.cfg.FutureDays; i++ {
		date := now.AddDate(0, 0, i).Format("2006-01-02")
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
