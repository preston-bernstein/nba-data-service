package snapshots

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/preston-bernstein/nba-data-service/internal/domain"
)

// Store defines how snapshots are loaded.
type Store interface {
	LoadGames(date string) (domain.TodayResponse, error)
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
func (s *FSStore) LoadGames(date string) (domain.TodayResponse, error) {
	if s == nil {
		return domain.TodayResponse{}, errors.New("snapshot store not configured")
	}
	if date == "" {
		return domain.TodayResponse{}, errors.New("snapshot date required")
	}
	path := filepath.Join(s.basePath, "games", fmt.Sprintf("%s.json", date))
	f, err := os.Open(path)
	if err != nil {
		return domain.TodayResponse{}, err
	}
	defer f.Close()

	var payload domain.TodayResponse
	if err := json.NewDecoder(f).Decode(&payload); err != nil {
		return domain.TodayResponse{}, err
	}
	// If the snapshot omits date, set it from the filename for safety.
	if payload.Date == "" {
		payload.Date = date
	}
	return payload, nil
}
