package teststubs

import (
	"context"
	"errors"
	"sync/atomic"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
)

// StubProvider is a test double for providers.GameProvider.
type StubProvider struct {
	Games  []domaingames.Game
	Err    error
	Calls  atomic.Int32
	Notify chan struct{}
}

// FetchGames returns configured games and error while tracking calls.
func (s *StubProvider) FetchGames(ctx context.Context, date string, tz string) ([]domaingames.Game, error) {
	_ = ctx
	_ = date
	_ = tz
	if s.Notify != nil {
		select {
		case <-s.Notify:
		default:
			close(s.Notify)
		}
	}
	s.Calls.Add(1)
	return s.Games, s.Err
}

// StubSnapshotStore is a test double for snapshots.Store.
type StubSnapshotStore struct {
	Games    map[string]domaingames.TodayResponse // keyed by date
	LoadErr  error
	FindGame *domaingames.Game
}

// LoadGames returns games for the given date if present in the Games map.
func (s *StubSnapshotStore) LoadGames(date string) (domaingames.TodayResponse, error) {
	if s.LoadErr != nil {
		return domaingames.TodayResponse{}, s.LoadErr
	}
	if s.Games == nil {
		return domaingames.TodayResponse{}, errors.New("snapshot not found")
	}
	resp, ok := s.Games[date]
	if !ok {
		return domaingames.TodayResponse{}, errors.New("snapshot not found")
	}
	return resp, nil
}

// FindGameByID searches the snapshot for the given date and returns the game if found.
func (s *StubSnapshotStore) FindGameByID(date, id string) (domaingames.Game, bool) {
	if s.FindGame != nil && s.FindGame.ID == id {
		return *s.FindGame, true
	}
	if s.Games == nil {
		return domaingames.Game{}, false
	}
	resp, ok := s.Games[date]
	if !ok {
		return domaingames.Game{}, false
	}
	for _, g := range resp.Games {
		if g.ID == id {
			return g, true
		}
	}
	return domaingames.Game{}, false
}

// StubSnapshotWriter is a test double for poller.SnapshotWriter.
type StubSnapshotWriter struct {
	Written map[string]domaingames.TodayResponse // keyed by date
	Err     error
}

// WriteGamesSnapshot records the snapshot for verification in tests.
func (w *StubSnapshotWriter) WriteGamesSnapshot(date string, snapshot domaingames.TodayResponse) error {
	if w.Err != nil {
		return w.Err
	}
	if w.Written == nil {
		w.Written = make(map[string]domaingames.TodayResponse)
	}
	w.Written[date] = snapshot
	return nil
}
