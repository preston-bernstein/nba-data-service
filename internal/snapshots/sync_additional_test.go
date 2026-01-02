package snapshots

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"log/slog"

	"nba-data-service/internal/domain"
	"nba-data-service/internal/providers"
)

type errProvider struct{ err error }

func (p errProvider) FetchGames(ctx context.Context, date string, tz string) ([]domain.Game, error) {
	return nil, p.err
}

type emptyProvider struct{}

func (emptyProvider) FetchGames(ctx context.Context, date string, tz string) ([]domain.Game, error) {
	return []domain.Game{}, nil
}

type goodProvider struct{ games []domain.Game }

func (p goodProvider) FetchGames(ctx context.Context, date string, tz string) ([]domain.Game, error) {
	return p.games, nil
}

func TestSyncerNormalizesConfig(t *testing.T) {
	s := NewSyncer(nil, nil, SyncConfig{
		Days:         0,
		FutureDays:   -1,
		Interval:     0,
		DailyHourUTC: -5,
	}, nil)

	if s.cfg.Days != 7 {
		t.Fatalf("expected default days 7, got %d", s.cfg.Days)
	}
	if s.cfg.FutureDays != 0 {
		t.Fatalf("expected future days clamped to 0, got %d", s.cfg.FutureDays)
	}
	if s.cfg.Interval <= 0 {
		t.Fatalf("expected interval defaulted, got %s", s.cfg.Interval)
	}
	if s.cfg.DailyHourUTC != 2 {
		t.Fatalf("expected daily hour defaulted to 2, got %d", s.cfg.DailyHourUTC)
	}
}

func TestFetchAndWriteHandlesErrorsAndSuccess(t *testing.T) {
	dir := t.TempDir()
	logger := testLogger()

	// Provider error -> logWarn path, no panic.
	s := NewSyncer(errProvider{err: providers.ErrProviderUnavailable}, NewWriter(dir, 7), SyncConfig{Enabled: true}, logger)
	s.fetchAndWrite(context.Background(), "2024-01-01")

	// Empty games -> logWarn path.
	s = NewSyncer(emptyProvider{}, NewWriter(dir, 7), SyncConfig{Enabled: true}, logger)
	s.fetchAndWrite(context.Background(), "2024-01-02")

	// Writer failure (basePath is a file, so MkdirAll should fail).
	filePath := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(filePath, []byte("x"), 0o644); err != nil {
		t.Fatalf("failed to create placeholder file: %v", err)
	}
	s = NewSyncer(goodProvider{games: []domain.Game{{ID: "g1"}}}, &Writer{basePath: filePath, retentionDays: 1}, SyncConfig{Enabled: true}, logger)
	s.fetchAndWrite(context.Background(), "2024-01-03")

	// Successful write path (large retention to avoid pruning).
	writer := NewWriter(t.TempDir(), 10000)
	s = NewSyncer(goodProvider{games: []domain.Game{{ID: "g2"}}}, writer, SyncConfig{Enabled: true}, logger)
	s.fetchAndWrite(context.Background(), "2024-01-04")
	requireSnapshotExists(t, writer, "2024-01-04")
}

// testLogger returns a no-op slog logger.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}
