package snapshots

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
)

// Store defines how snapshots are loaded.
type Store interface {
	LoadGames(date string) (domaingames.TodayResponse, error)
}

// FSStore loads snapshots from the filesystem.
type FSStore struct {
	basePath string
}

// NewFSStore constructs an FS-backed snapshot store rooted at basePath.
func NewFSStore(basePath string) *FSStore {
	return &FSStore{basePath: basePath}
}

// LoadGames reads a snapshot for the given date (YYYY-MM-DD) from disk.
// Files are expected at {basePath}/games/{date}.json with a TodayResponse payload.
func (s *FSStore) LoadGames(date string) (domaingames.TodayResponse, error) {
	var payload domaingames.TodayResponse
	if err := s.load(kindGames, date, &payload); err != nil {
		return domaingames.TodayResponse{}, err
	}
	if payload.Date == "" {
		payload.Date = date
	}
	return payload, nil
}

func (s *FSStore) load(kind snapshotKind, date string, payload any) error {
	if s == nil {
		return errors.New("snapshot store not configured")
	}
	if date == "" {
		return errors.New("snapshot date required")
	}
	path := filepath.Join(s.basePath, string(kind), fmt.Sprintf("%s.json", date))
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := json.NewDecoder(f).Decode(payload); err != nil {
		return err
	}
	return nil
}

func (s *FSStore) decodeFile(path string, payload any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewDecoder(f).Decode(payload)
}
