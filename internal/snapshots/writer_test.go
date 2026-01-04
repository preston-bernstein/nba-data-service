package snapshots

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	domainplayers "github.com/preston-bernstein/nba-data-service/internal/domain/players"
	domainteams "github.com/preston-bernstein/nba-data-service/internal/domain/teams"
)

func TestWriterWritesSnapshotAndManifest(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 10)

	today := time.Now().Format("2006-01-02")
	snap := domaingames.TodayResponse{
		Date:  today,
		Games: []domaingames.Game{{ID: "g1"}},
	}

	writeSnapshot(t, w, today, snap)

	// Verify snapshot file exists.
	data, err := os.ReadFile(filepath.Join(dir, "games", today+".json"))
	if err != nil {
		t.Fatalf("expected snapshot file, got err %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("expected snapshot content")
	}

	// Verify manifest was written.
	mBytes, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatalf("expected manifest, got err %v", err)
	}
	if len(mBytes) == 0 {
		t.Fatalf("expected manifest content")
	}
}

func TestWriterPrunesOldSnapshots(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 1) // 1-day retention

	oldDate := time.Now().AddDate(0, 0, -5).Format("2006-01-02")
	newDate := time.Now().Format("2006-01-02")

	// Write an old snapshot and a new one.
	for _, d := range []string{oldDate, newDate} {
		snap := domaingames.TodayResponse{
			Date:  d,
			Games: []domaingames.Game{{ID: d}},
		}
		writeSnapshot(t, w, d, snap)
	}

	// Old snapshot should be pruned.
	if _, err := os.Stat(filepath.Join(dir, "games", oldDate+".json")); err == nil {
		t.Fatalf("expected old snapshot to be pruned")
	}
	if _, err := os.Stat(filepath.Join(dir, "games", newDate+".json")); err != nil {
		t.Fatalf("expected new snapshot to exist")
	}
}

func TestWriterHandlesNilAndEmptyDate(t *testing.T) {
	var w *Writer
	if err := w.WriteGamesSnapshot("2024-01-01", domaingames.TodayResponse{}); err == nil {
		t.Fatalf("expected error for nil writer")
	}

	w = NewWriter(t.TempDir(), 1)
	if err := w.WriteGamesSnapshot("", domaingames.TodayResponse{}); err == nil {
		t.Fatalf("expected error for empty date")
	}
}

func TestNewWriterDefaultsRetention(t *testing.T) {
	w := NewWriter(t.TempDir(), 0)
	if w.retentionDays <= 0 {
		t.Fatalf("expected retention to default when non-positive provided")
	}
}

func TestListDatesIgnoresNonJSONAndDirs(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "games", "nested"), 0o755); err != nil {
		t.Fatalf("failed to create nested dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "games", "2024-01-01.json"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("failed to write snapshot: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "games", "ignore.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("failed to write extra file: %v", err)
	}

	w := NewWriter(dir, 1)
	dates, err := w.listDates(kindGames)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(dates) != 1 || dates[0] != "2024-01-01" {
		t.Fatalf("expected only json snapshots, got %v", dates)
	}
}

func TestBasePathExposesRoot(t *testing.T) {
	base := t.TempDir()
	w := NewWriter(base, 1)
	if w.BasePath() != base {
		t.Fatalf("expected base path %s, got %s", base, w.BasePath())
	}
}

func TestSnapshotPathUsesDateAndBase(t *testing.T) {
	base := t.TempDir()
	w := NewWriter(base, 1)
	path := w.snapshotPath(kindGames, "2024-01-01")
	if path == "" || path == base {
		t.Fatalf("expected snapshot path to include games file, got %s", path)
	}
}

func TestWriterHandlesNilOtel(t *testing.T) {
	w := NewWriter(t.TempDir(), 1)
	// Should not panic when otel instruments are nil inside recorder; covered via WriteGamesSnapshot path.
	if err := w.WriteGamesSnapshot("2024-02-02", domaingames.TodayResponse{}); err != nil {
		t.Fatalf("expected write to succeed, got %v", err)
	}
}

func TestListGameDatesMissingDirReturnsEmpty(t *testing.T) {
	w := NewWriter(t.TempDir(), 1)
	dates, err := w.listDates(kindGames)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(dates) != 0 {
		t.Fatalf("expected empty dates, got %v", dates)
	}
}

func TestPruneOldSnapshotsKeepsRecent(t *testing.T) {
	w := NewWriter(t.TempDir(), 1)
	today := time.Now().Format("2006-01-02")
	old := time.Now().AddDate(0, 0, -10).Format("2006-01-02")
	writeSnapshot(t, w, today, simpleSnapshot(today))
	writeSnapshot(t, w, old, simpleSnapshot(old))
	keep, err := w.pruneOldSnapshots(kindGames, []string{today, old})
	if err != nil {
		t.Fatalf("expected prune to succeed: %v", err)
	}
	if len(keep) != 1 || keep[0] != today {
		t.Fatalf("expected only today's snapshot kept, got %v", keep)
	}
}

func TestPruneOldSnapshotsKeepsUnparsableDates(t *testing.T) {
	w := NewWriter(t.TempDir(), 1)
	keep, err := w.pruneOldSnapshots(kindGames, []string{"bad-date"})
	if err != nil {
		t.Fatalf("expected prune to succeed: %v", err)
	}
	if len(keep) != 1 || keep[0] != "bad-date" {
		t.Fatalf("expected unparsable date kept, got %v", keep)
	}
}

func TestManifestWrittenWithRetentionAndDates(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 3)
	date := "2024-03-03"
	if err := w.WriteGamesSnapshot(date, simpleSnapshot(date)); err != nil {
		t.Fatalf("write snapshot failed: %v", err)
	}
	m, err := readManifest(filepath.Join(dir, "manifest.json"), 0)
	if err != nil {
		t.Fatalf("expected manifest read: %v", err)
	}
	if m.Retention.GamesDays != 3 {
		t.Fatalf("expected retention 3, got %d", m.Retention.GamesDays)
	}
	if m.Games.LastRefreshed.IsZero() {
		t.Fatalf("expected lastRefreshed to be set")
	}
}

func TestTeamsAndPlayersSnapshotsSortedAndManifested(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 5)
	date := time.Now().UTC().Format("2006-01-02")

	teamsSnap := TeamsSnapshot{
		Teams: []domainteams.Team{
			{ID: "2", Name: "B Team"},
			{ID: "1", Name: "A Team"},
		},
	}
	if err := w.WriteTeamsSnapshot(date, teamsSnap); err != nil {
		t.Fatalf("failed to write teams snapshot: %v", err)
	}

	playersSnap := PlayersSnapshot{
		Players: []domainplayers.Player{
			{ID: "p3", Team: domainteams.Team{ID: "1"}, LastName: "Z", FirstName: "A"},
			{ID: "p1", Team: domainteams.Team{ID: "1"}, LastName: "A", FirstName: "B"},
			{ID: "p2", Team: domainteams.Team{ID: "2"}, LastName: "A", FirstName: "A"},
		},
	}
	if err := w.WritePlayersSnapshot(date, playersSnap); err != nil {
		t.Fatalf("failed to write players snapshot: %v", err)
	}

	m, err := readManifest(filepath.Join(dir, "manifest.json"), 0)
	if err != nil {
		t.Fatalf("expected manifest read: %v", err)
	}
	if len(m.Teams.Dates) != 1 || m.Teams.Dates[0] != date {
		t.Fatalf("expected teams date recorded, got %v", m.Teams.Dates)
	}
	if len(m.Players.Dates) != 1 || m.Players.Dates[0] != date {
		t.Fatalf("expected players date recorded, got %v", m.Players.Dates)
	}

	teamsPayload, err := os.ReadFile(filepath.Join(dir, "teams", date+".json"))
	if err != nil {
		t.Fatalf("failed to read teams snapshot: %v", err)
	}
	var decodedTeams TeamsSnapshot
	if err := json.Unmarshal(teamsPayload, &decodedTeams); err != nil {
		t.Fatalf("failed to decode teams snapshot: %v", err)
	}
	if decodedTeams.Teams[0].ID != "1" || decodedTeams.Teams[1].ID != "2" {
		t.Fatalf("expected teams sorted by id, got %+v", decodedTeams.Teams)
	}

	playersPayload, err := os.ReadFile(filepath.Join(dir, "players", date+".json"))
	if err != nil {
		t.Fatalf("failed to read players snapshot: %v", err)
	}
	var decodedPlayers PlayersSnapshot
	if err := json.Unmarshal(playersPayload, &decodedPlayers); err != nil {
		t.Fatalf("failed to decode players snapshot: %v", err)
	}
	if decodedPlayers.Players[0].ID != "p1" || decodedPlayers.Players[1].ID != "p3" || decodedPlayers.Players[2].ID != "p2" {
		t.Fatalf("expected players sorted, got %+v", decodedPlayers.Players)
	}
}

func TestBasePathNilWriter(t *testing.T) {
	var w *Writer
	if w.BasePath() != "" {
		t.Fatalf("expected empty base path for nil writer")
	}
}

func TestWriteGamesSnapshotWriteFileError(t *testing.T) {
	dir := t.TempDir()
	gamesDir := filepath.Join(dir, "games")
	if err := os.MkdirAll(gamesDir, 0o555); err != nil {
		t.Fatalf("failed to create games dir: %v", err)
	}
	w := NewWriter(dir, 1)
	if err := w.WriteGamesSnapshot("2024-01-01", simpleSnapshot("2024-01-01")); err == nil {
		t.Fatalf("expected write error when games dir is read-only")
	}
}

func TestWriteGamesSnapshotRenameError(t *testing.T) {
	dir := t.TempDir()
	// Create a directory at the target path to force rename to fail.
	targetDir := filepath.Join(dir, "games", "2024-01-01.json")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("failed to create target dir: %v", err)
	}
	w := NewWriter(dir, 1)
	if err := w.WriteGamesSnapshot("2024-01-01", simpleSnapshot("2024-01-01")); err == nil {
		t.Fatalf("expected rename error when target is a directory")
	}
}

func TestListGameDatesErrorWhenGamesNotDir(t *testing.T) {
	dir := t.TempDir()
	// Create a file named "games" to trigger ReadDir error (not IsNotExist).
	if err := os.WriteFile(filepath.Join(dir, "games"), []byte("not a dir"), 0o644); err != nil {
		t.Fatalf("failed to create games file: %v", err)
	}
	w := NewWriter(dir, 1)
	if _, err := w.listDates(kindGames); err == nil {
		t.Fatalf("expected error when games path is a file")
	}
}

func TestWriteGamesSnapshotSetsDateWhenMissing(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 10000)
	snap := domaingames.TodayResponse{Games: []domaingames.Game{{ID: "g1"}}}
	if err := w.WriteGamesSnapshot("2024-04-04", snap); err != nil {
		t.Fatalf("write snapshot failed: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "games", "2024-04-04.json"))
	if err != nil {
		t.Fatalf("expected snapshot file: %v", err)
	}
	var loaded domaingames.TodayResponse
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("failed to decode snapshot: %v", err)
	}
	if loaded.Date != "2024-04-04" {
		t.Fatalf("expected date to be set, got %s", loaded.Date)
	}
}

func TestWriteSnapshotsSkipWhenUnchanged(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 10000)
	date := time.Now().UTC().Format("2006-01-02")

	if err := w.WriteGamesSnapshot(date, simpleSnapshot(date)); err != nil {
		t.Fatalf("write snapshot failed: %v", err)
	}
	gamePath := filepath.Join(dir, "games", date+".json")
	initialInfo, err := os.Stat(gamePath)
	if err != nil {
		t.Fatalf("expected game snapshot file: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if err := w.WriteGamesSnapshot(date, simpleSnapshot(date)); err != nil {
		t.Fatalf("second write failed: %v", err)
	}
	info, err := os.Stat(gamePath)
	if err != nil {
		t.Fatalf("expected game snapshot file: %v", err)
	}
	if !info.ModTime().Equal(initialInfo.ModTime()) {
		t.Fatalf("expected game snapshot to remain unchanged")
	}

	teamSnap := TeamsSnapshot{Date: date, Teams: []domainteams.Team{{ID: "t1", Name: "Team 1"}}}
	if err := w.WriteTeamsSnapshot(date, teamSnap); err != nil {
		t.Fatalf("write teams failed: %v", err)
	}
	teamPath := filepath.Join(dir, "teams", date+".json")
	teamInfo, err := os.Stat(teamPath)
	if err != nil {
		t.Fatalf("expected teams snapshot file: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if err := w.WriteTeamsSnapshot(date, teamSnap); err != nil {
		t.Fatalf("second teams write failed: %v", err)
	}
	teamInfo2, err := os.Stat(teamPath)
	if err != nil {
		t.Fatalf("expected teams snapshot file: %v", err)
	}
	if !teamInfo2.ModTime().Equal(teamInfo.ModTime()) {
		t.Fatalf("expected teams snapshot to remain unchanged")
	}

	playerSnap := PlayersSnapshot{Date: date, Players: []domainplayers.Player{{ID: "p1", Team: domainteams.Team{ID: "t1"}, FirstName: "A", LastName: "B"}}}
	if err := w.WritePlayersSnapshot(date, playerSnap); err != nil {
		t.Fatalf("write players failed: %v", err)
	}
	playerPath := filepath.Join(dir, "players", date+".json")
	playerInfo, err := os.Stat(playerPath)
	if err != nil {
		t.Fatalf("expected players snapshot file: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if err := w.WritePlayersSnapshot(date, playerSnap); err != nil {
		t.Fatalf("second players write failed: %v", err)
	}
	playerInfo2, err := os.Stat(playerPath)
	if err != nil {
		t.Fatalf("expected players snapshot file: %v", err)
	}
	if !playerInfo2.ModTime().Equal(playerInfo.ModTime()) {
		t.Fatalf("expected players snapshot to remain unchanged")
	}
}

func TestWriteGamesAndPlayersSetDateAndSort(t *testing.T) {
	dir := t.TempDir()
	w := NewWriter(dir, 10000)
	date := time.Now().UTC().Format("2006-01-02")
	gamesSnap := domaingames.TodayResponse{
		Games: []domaingames.Game{
			{ID: "b"},
			{ID: "a"},
		},
	}
	if err := w.WriteGamesSnapshot(date, gamesSnap); err != nil {
		t.Fatalf("write games snapshot failed: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "games", date+".json"))
	if err != nil {
		t.Fatalf("expected games snapshot file: %v", err)
	}
	var loaded domaingames.TodayResponse
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("failed to decode games snapshot: %v", err)
	}
	if loaded.Date != date || loaded.Games[0].ID != "a" {
		t.Fatalf("expected games sorted with date set, got %+v", loaded)
	}

	playerSnap := PlayersSnapshot{
		Players: []domainplayers.Player{
			{ID: "p2", Team: domainteams.Team{ID: "t1"}, LastName: "Z"},
			{ID: "p1", Team: domainteams.Team{ID: "t1"}, LastName: "A"},
		},
	}
	if err := w.WritePlayersSnapshot(date, playerSnap); err != nil {
		t.Fatalf("write players snapshot failed: %v", err)
	}
	pdata, err := os.ReadFile(filepath.Join(dir, "players", date+".json"))
	if err != nil {
		t.Fatalf("expected players snapshot file: %v", err)
	}
	var loadedPlayers PlayersSnapshot
	if err := json.Unmarshal(pdata, &loadedPlayers); err != nil {
		t.Fatalf("failed to decode players snapshot: %v", err)
	}
	if loadedPlayers.Date != date || loadedPlayers.Players[0].ID != "p1" {
		t.Fatalf("expected players sorted with date set, got %+v", loadedPlayers)
	}
}

func TestWriteTeamsAndPlayersSnapshotRequireDate(t *testing.T) {
	w := NewWriter(t.TempDir(), 7)
	if err := w.WriteTeamsSnapshot("", TeamsSnapshot{}); err == nil {
		t.Fatalf("expected error for empty team snapshot date")
	}
	if err := w.WritePlayersSnapshot("", PlayersSnapshot{}); err == nil {
		t.Fatalf("expected error for empty player snapshot date")
	}
	var nilWriter *Writer
	if err := nilWriter.WriteTeamsSnapshot("2024-01-01", TeamsSnapshot{}); err == nil {
		t.Fatalf("expected error for nil writer teams")
	}
	if err := nilWriter.WritePlayersSnapshot("2024-01-01", PlayersSnapshot{}); err == nil {
		t.Fatalf("expected error for nil writer players")
	}
}
