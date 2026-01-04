package snapshots

import (
	"context"
	"testing"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	domainplayers "github.com/preston-bernstein/nba-data-service/internal/domain/players"
	domainteams "github.com/preston-bernstein/nba-data-service/internal/domain/teams"
)

type fakeProvider struct {
	dates   []string
	teams   int
	players int
}

func (p *fakeProvider) FetchGames(ctx context.Context, date string, _ string) ([]domaingames.Game, error) {
	p.dates = append(p.dates, date)
	return []domaingames.Game{
		{ID: date + "-1", Provider: "stub"},
	}, nil
}

func (p *fakeProvider) FetchTeams(ctx context.Context) ([]domainteams.Team, error) {
	p.teams++
	return []domainteams.Team{{ID: "t1"}}, nil
}

func (p *fakeProvider) FetchPlayers(ctx context.Context) ([]domainplayers.Player, error) {
	p.players++
	return []domainplayers.Player{{ID: "p1", Team: domainteams.Team{ID: "t1"}}}, nil
}

type dataProviderStub struct {
	fakeProvider
}

func (p *dataProviderStub) FetchGames(ctx context.Context, date string, _ string) ([]domaingames.Game, error) {
	p.dates = append(p.dates, date)
	return []domaingames.Game{{ID: date + "-g"}}, nil
}

// Implement Team/Player fetch via embedded fakeProvider.

type fakeRosterStore struct {
	teams   []domainteams.Team
	players []domainplayers.Player
}

func (f *fakeRosterStore) SetTeams(items []domainteams.Team)       { f.teams = items }
func (f *fakeRosterStore) SetPlayers(items []domainplayers.Player) { f.players = items }

func TestSyncerBackfillsPastAndFuture(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer := NewWriter(t.TempDir(), 10000)
	now := time.Date(2024, 1, 10, 12, 0, 0, 0, time.UTC)
	provider := &fakeProvider{}
	cfg := SyncConfig{
		Enabled:    true,
		Days:       3,
		FutureDays: 2,
		Interval:   time.Nanosecond,
	}

	// Seed snapshots: yesterday (will still refresh), 2 days back (should skip),
	// and future +2 (should skip).
	writeSimpleSnapshot(t, writer, "2024-01-09")
	writeSimpleSnapshot(t, writer, "2024-01-08")
	writeSimpleSnapshot(t, writer, "2024-01-12")

	roster := &fakeRosterStore{}
	syncer := NewSyncer(provider, writer, cfg, nil, roster)
	syncer.now = func() time.Time { return now }

	syncer.Run(ctx)
	cancel()

	expected := []string{"2024-01-10", "2024-01-09", "2024-01-11"}

	assertDatesEqual(t, provider.dates, expected)
	for _, date := range expected {
		requireSnapshotExists(t, writer, date)
	}
	// Ensure previously existing snapshots remain.
	requireSnapshotExists(t, writer, "2024-01-08")
	requireSnapshotExists(t, writer, "2024-01-12")

	if provider.teams == 0 || provider.players == 0 {
		t.Fatalf("expected teams and players to be synced at least once")
	}
	if len(roster.teams) != 1 || len(roster.players) != 1 {
		t.Fatalf("expected roster store to be updated")
	}
}

type disabledProvider struct{}

func (disabledProvider) FetchGames(ctx context.Context, date string, tz string) ([]domaingames.Game, error) {
	return nil, nil
}

func (disabledProvider) FetchTeams(ctx context.Context) ([]domainteams.Team, error) {
	return nil, nil
}

func (disabledProvider) FetchPlayers(ctx context.Context) ([]domainplayers.Player, error) {
	return nil, nil
}

func TestSyncerSkipsWhenDisabledOrNil(t *testing.T) {
	s := NewSyncer(nil, nil, SyncConfig{Enabled: false}, nil, nil)
	s.Run(context.Background())

	s = NewSyncer(disabledProvider{}, nil, SyncConfig{Enabled: true}, nil, nil)
	s.Run(context.Background())
}

func TestSyncerSleepRespectsContext(t *testing.T) {
	s := NewSyncer(nil, nil, SyncConfig{Enabled: true}, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	s.sleep(ctx, time.Second)
	if time.Since(start) > 50*time.Millisecond {
		t.Fatalf("expected sleep to return quickly when context canceled")
	}
}

func TestHasSnapshotNilWriter(t *testing.T) {
	s := NewSyncer(nil, nil, SyncConfig{}, nil, nil)
	if s.hasSnapshot(kindGames, "2024-01-01") {
		t.Fatalf("expected hasSnapshot to be false with nil writer")
	}
}

func TestBuildDatesSkipsExistingSnapshots(t *testing.T) {
	w := NewWriter(t.TempDir(), 10000)
	writeSimpleSnapshot(t, w, "2024-01-03") // past (beyond yesterday)
	writeSimpleSnapshot(t, w, "2024-01-06") // future

	s := NewSyncer(nil, w, SyncConfig{Enabled: true, Days: 5, FutureDays: 2}, nil, nil)
	now := time.Date(2024, 1, 5, 10, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	dates := s.buildDates(s.now())

	want := map[string]bool{
		"2024-01-05": true, // today
		"2024-01-04": true, // yesterday
	}
	for _, d := range dates {
		if want[d] {
			delete(want, d)
		}
		if d == "2024-01-03" || d == "2024-01-06" {
			t.Fatalf("expected existing snapshots to be skipped, got %s", d)
		}
	}
	if len(want) != 0 {
		t.Fatalf("expected today and yesterday to be present, missing %v", want)
	}
}

func TestSyncerRespectsStaticRefreshIntervals(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 30)
	now := time.Date(2024, 1, 10, 12, 0, 0, 0, time.UTC)
	provider := &fakeProvider{}
	cfg := SyncConfig{
		Enabled:             true,
		Days:                2,
		FutureDays:          0,
		Interval:            time.Nanosecond,
		TeamsRefreshDays:    7,
		PlayersRefreshHours: 24,
	}
	s := NewSyncer(provider, w, cfg, nil, &fakeRosterStore{})

	// Fresh manifest: should skip because refreshed just now.
	m := defaultManifest(30)
	m.Teams.LastRefreshed = now
	m.Players.LastRefreshed = now
	if err := writeManifest(dir, m); err != nil {
		t.Fatalf("failed to seed manifest: %v", err)
	}
	s.syncStatic(context.Background(), now)
	if provider.teams != 0 || provider.players != 0 {
		t.Fatalf("expected no static fetch when within refresh window")
	}

	// Make manifest stale to force refresh.
	m.Teams.LastRefreshed = now.AddDate(0, 0, -8)
	m.Players.LastRefreshed = now.Add(-25 * time.Hour)
	if err := writeManifest(dir, m); err != nil {
		t.Fatalf("failed to update manifest: %v", err)
	}
	s.syncStatic(context.Background(), now)
	if provider.teams == 0 || provider.players == 0 {
		t.Fatalf("expected static fetch when stale")
	}
}

func TestSyncerDailyUsesTicker(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 5)
	prov := &dataProviderStub{}
	cfg := SyncConfig{
		Enabled:             true,
		Days:                2,
		FutureDays:          0,
		Interval:            time.Nanosecond,
		DailyHourUTC:        time.Now().UTC().Hour(),
		TeamsRefreshDays:    1,
		PlayersRefreshHours: 1,
	}
	s := NewSyncer(prov, w, cfg, nil, &fakeRosterStore{})
	s.now = func() time.Time { return time.Now().UTC() }
	s.newTicker = func(d time.Duration) *time.Ticker {
		return time.NewTicker(time.Millisecond)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.daily(ctx)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	if len(prov.dates) == 0 || prov.teams == 0 || prov.players == 0 {
		t.Fatalf("expected daily loop to trigger sync once, got dates=%v teams=%d players=%d", prov.dates, prov.teams, prov.players)
	}
}

func TestDailyReturnsOnCancel(t *testing.T) {
	s := NewSyncer(nil, NewWriter(t.TempDir(), 1), SyncConfig{Enabled: true}, nil, nil)
	s.newTicker = func(d time.Duration) *time.Ticker { return time.NewTicker(time.Hour) }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.daily(ctx) // should exit immediately without blocking
}

func TestShouldRefreshDefaultsToTrueWhenNeverRefreshed(t *testing.T) {
	dir := t.TempDir()
	writer := NewWriter(dir, 7)
	s := NewSyncer(nil, writer, SyncConfig{Enabled: true, TeamsRefreshDays: 7, PlayersRefreshHours: 24}, nil, nil)
	now := time.Now().UTC()

	if !s.shouldRefresh(kindTeams, now) {
		t.Fatalf("expected teams refresh when never refreshed")
	}
	if !s.shouldRefresh(kindPlayers, now) {
		t.Fatalf("expected players refresh when never refreshed")
	}
}
