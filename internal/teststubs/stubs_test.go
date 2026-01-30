package teststubs

import (
	"context"
	"errors"
	"testing"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
)

func TestStubProviderTracksCalls(t *testing.T) {
	err := errors.New("boom")
	p := &StubProvider{Games: []domaingames.Game{{ID: "g1"}}, Err: err}
	if _, got := p.FetchGames(context.Background(), "2024-01-01", ""); !errors.Is(got, err) {
		t.Fatalf("expected error passthrough, got %v", got)
	}
	if p.Calls.Load() != 1 {
		t.Fatalf("expected call count 1, got %d", p.Calls.Load())
	}
}

func TestStubProviderNotifyChannel(t *testing.T) {
	notify := make(chan struct{})
	p := &StubProvider{
		Games:  []domaingames.Game{{ID: "g1"}},
		Notify: notify,
	}

	// First call should close the channel
	_, _ = p.FetchGames(context.Background(), "2024-01-01", "")

	// Verify channel was closed
	select {
	case <-notify:
		// Channel closed as expected
	default:
		t.Fatal("expected notify channel to be closed")
	}

	// Second call should handle already-closed channel gracefully
	_, _ = p.FetchGames(context.Background(), "2024-01-01", "")
	if p.Calls.Load() != 2 {
		t.Fatalf("expected 2 calls, got %d", p.Calls.Load())
	}
}

func TestStubSnapshotStore(t *testing.T) {
	date := "2024-01-01"
	s := &StubSnapshotStore{
		Games: map[string]domaingames.TodayResponse{
			date: domaingames.NewTodayResponse(date, []domaingames.Game{{ID: "g1"}}),
		},
	}

	resp, err := s.LoadGames(date)
	if err != nil || resp.Date != date {
		t.Fatalf("expected loaded games, got %v err %v", resp, err)
	}

	game, ok := s.FindGameByID(date, "g1")
	if !ok || game.ID != "g1" {
		t.Fatalf("expected game found, got %v ok=%v", game, ok)
	}

	_, ok = s.FindGameByID(date, "missing")
	if ok {
		t.Fatalf("expected game not found")
	}
}

func TestStubSnapshotStoreLoadErr(t *testing.T) {
	loadErr := errors.New("load error")
	s := &StubSnapshotStore{LoadErr: loadErr}

	_, err := s.LoadGames("2024-01-01")
	if !errors.Is(err, loadErr) {
		t.Fatalf("expected LoadErr to be returned, got %v", err)
	}
}

func TestStubSnapshotStoreNilGames(t *testing.T) {
	s := &StubSnapshotStore{} // nil Games map

	_, err := s.LoadGames("2024-01-01")
	if err == nil {
		t.Fatal("expected error for nil Games map")
	}
}

func TestStubSnapshotStoreDateNotFound(t *testing.T) {
	s := &StubSnapshotStore{
		Games: map[string]domaingames.TodayResponse{
			"2024-01-01": domaingames.NewTodayResponse("2024-01-01", nil),
		},
	}

	_, err := s.LoadGames("2024-01-02") // different date
	if err == nil {
		t.Fatal("expected error for missing date")
	}
}

func TestStubSnapshotStoreFindGameShortcut(t *testing.T) {
	game := domaingames.Game{ID: "shortcut-game"}
	s := &StubSnapshotStore{FindGame: &game}

	found, ok := s.FindGameByID("any-date", "shortcut-game")
	if !ok || found.ID != "shortcut-game" {
		t.Fatalf("expected FindGame shortcut to return game, got %v ok=%v", found, ok)
	}

	// ID mismatch should fall through
	_, ok = s.FindGameByID("any-date", "other-id")
	if ok {
		t.Fatal("expected no match when FindGame ID doesn't match")
	}
}

func TestStubSnapshotStoreFindGameNilGames(t *testing.T) {
	s := &StubSnapshotStore{} // nil Games map, no FindGame

	_, ok := s.FindGameByID("2024-01-01", "g1")
	if ok {
		t.Fatal("expected not found for nil Games map")
	}
}

func TestStubSnapshotStoreFindGameDateNotFound(t *testing.T) {
	s := &StubSnapshotStore{
		Games: map[string]domaingames.TodayResponse{
			"2024-01-01": domaingames.NewTodayResponse("2024-01-01", []domaingames.Game{{ID: "g1"}}),
		},
	}

	_, ok := s.FindGameByID("2024-01-02", "g1") // wrong date
	if ok {
		t.Fatal("expected not found for missing date")
	}
}

func TestStubSnapshotWriter(t *testing.T) {
	date := "2024-01-01"
	w := &StubSnapshotWriter{}
	err := w.WriteGamesSnapshot(date, domaingames.NewTodayResponse(date, []domaingames.Game{{ID: "g1"}}))
	if err != nil {
		t.Fatalf("expected write success, got %v", err)
	}
	if len(w.Written) != 1 {
		t.Fatalf("expected one written entry, got %d", len(w.Written))
	}

	w.Err = errors.New("write error")
	err = w.WriteGamesSnapshot("2024-01-02", domaingames.NewTodayResponse("2024-01-02", []domaingames.Game{{ID: "g2"}}))
	if err == nil {
		t.Fatalf("expected write error")
	}
}
