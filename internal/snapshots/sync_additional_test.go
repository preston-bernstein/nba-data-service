package snapshots

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"log/slog"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	domainplayers "github.com/preston-bernstein/nba-data-service/internal/domain/players"
	domainteams "github.com/preston-bernstein/nba-data-service/internal/domain/teams"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
)

type errProvider struct{ err error }

func (p errProvider) FetchGames(ctx context.Context, date string, tz string) ([]domaingames.Game, error) {
	return nil, p.err
}

func (p errProvider) FetchTeams(ctx context.Context) ([]domainteams.Team, error) {
	return nil, p.err
}

func (p errProvider) FetchPlayers(ctx context.Context) ([]domainplayers.Player, error) {
	return nil, p.err
}

type emptyProvider struct{}

func (emptyProvider) FetchGames(ctx context.Context, date string, tz string) ([]domaingames.Game, error) {
	return []domaingames.Game{}, nil
}

func (emptyProvider) FetchTeams(ctx context.Context) ([]domainteams.Team, error) {
	return nil, nil
}

func (emptyProvider) FetchPlayers(ctx context.Context) ([]domainplayers.Player, error) {
	return nil, nil
}

type goodProvider struct{ games []domaingames.Game }

func (p goodProvider) FetchGames(ctx context.Context, date string, tz string) ([]domaingames.Game, error) {
	return p.games, nil
}

func (goodProvider) FetchTeams(ctx context.Context) ([]domainteams.Team, error) {
	return nil, nil
}

func (goodProvider) FetchPlayers(ctx context.Context) ([]domainplayers.Player, error) {
	return nil, nil
}

func TestSyncerNormalizesConfig(t *testing.T) {
	s := NewSyncer(nil, nil, SyncConfig{
		Days:         0,
		FutureDays:   -1,
		Interval:     0,
		DailyHourUTC: -5,
	}, nil, nil)

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
	s := NewSyncer(errProvider{err: providers.ErrProviderUnavailable}, NewWriter(dir, 7), SyncConfig{Enabled: true}, logger, nil)
	s.fetchAndWrite(context.Background(), "2024-01-01")

	// Empty games -> logWarn path.
	s = NewSyncer(emptyProvider{}, NewWriter(dir, 7), SyncConfig{Enabled: true}, logger, nil)
	s.fetchAndWrite(context.Background(), "2024-01-02")

	// Writer failure (basePath is a file, so MkdirAll should fail).
	filePath := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(filePath, []byte("x"), 0o644); err != nil {
		t.Fatalf("failed to create placeholder file: %v", err)
	}
	s = NewSyncer(goodProvider{games: []domaingames.Game{{ID: "g1"}}}, &Writer{basePath: filePath, retentionDays: 1}, SyncConfig{Enabled: true}, logger, nil)
	s.fetchAndWrite(context.Background(), "2024-01-03")

	// Successful write path (large retention to avoid pruning).
	writer := NewWriter(t.TempDir(), 10000)
	s = NewSyncer(goodProvider{games: []domaingames.Game{{ID: "g2"}}}, writer, SyncConfig{Enabled: true}, logger, nil)
	s.fetchAndWrite(context.Background(), "2024-01-04")
	requireSnapshotExists(t, writer, "2024-01-04")
}

func TestRunSkipsWhenDisabled(t *testing.T) {
	prov := goodProvider{games: []domaingames.Game{{ID: "g1"}}}
	writer := NewWriter(t.TempDir(), 7)
	s := NewSyncer(prov, writer, SyncConfig{Enabled: false}, testLogger(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run(ctx) // should return immediately without panic
}

func TestBackfillRespectsContextCancel(t *testing.T) {
	prov := goodProvider{games: []domaingames.Game{{ID: "g1"}}}
	writer := NewWriter(t.TempDir(), 7)
	s := NewSyncer(prov, writer, SyncConfig{Enabled: true, Interval: time.Second}, testLogger(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.backfill(ctx, time.Now().UTC()) // should exit quickly without writing
}

func TestSyncTeamsAndPlayersSkipWithoutProvider(t *testing.T) {
	s := NewSyncer(nil, NewWriter(t.TempDir(), 7), SyncConfig{Enabled: true}, testLogger(), nil)
	s.syncTeams(context.Background(), time.Now().UTC(), "2024-01-01")
	s.syncPlayers(context.Background(), time.Now().UTC(), "2024-01-01")
}

func TestSyncTeamsAndPlayersHandleWriteError(t *testing.T) {
	tempFile := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(tempFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	writer := &Writer{basePath: tempFile, retentionDays: 1}
	prov := &dataProviderStub{}
	s := NewSyncer(prov, writer, SyncConfig{Enabled: true}, testLogger(), nil)
	s.syncTeams(context.Background(), time.Now().UTC(), "2024-01-01")
	s.syncPlayers(context.Background(), time.Now().UTC(), "2024-01-01")
}

func TestSyncTeamsAndPlayersUpdateRosterStore(t *testing.T) {
	writer := NewWriter(t.TempDir(), 7)
	prov := &fakeProvider{}
	roster := &fakeRosterStore{}
	s := NewSyncer(prov, writer, SyncConfig{Enabled: true, TeamsRefreshDays: 1, PlayersRefreshHours: 1}, testLogger(), roster)
	s.syncTeams(context.Background(), time.Now().UTC(), "2024-01-01")
	s.syncPlayers(context.Background(), time.Now().UTC(), "2024-01-01")

	if len(roster.teams) != 1 {
		t.Fatalf("expected roster store teams updated, got %d", len(roster.teams))
	}
	if len(roster.players) != 1 {
		t.Fatalf("expected roster store players updated, got %d", len(roster.players))
	}
}

// testLogger returns a no-op slog logger.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}
