package snapshots

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// Manifest tracks snapshot metadata.
type Manifest struct {
	Version     int       `json:"version"`
	GeneratedAt time.Time `json:"generatedAt"`
	Retention   Retention `json:"retention"`
	Games       GamesMeta `json:"games"`
}

type Retention struct {
	GamesDays int `json:"gamesDays"`
}

type GamesMeta struct {
	Dates         []string  `json:"dates"`
	LastRefreshed time.Time `json:"lastRefreshed"`
}

func defaultManifest(retentionDays int) Manifest {
	return Manifest{
		Version:     1,
		GeneratedAt: time.Now().UTC(),
		Retention: Retention{
			GamesDays: retentionDays,
		},
		Games: GamesMeta{
			Dates:         []string{},
			LastRefreshed: time.Time{},
		},
	}
}

func readManifest(path string, retentionDays int) (Manifest, error) {
	f, err := os.Open(path) //nolint:gosec // G304: path comes from config or a test temp dir
	if err != nil {
		return defaultManifest(retentionDays), err
	}
	defer func() {
		_ = f.Close()
	}()
	var m Manifest
	if err := json.NewDecoder(f).Decode(&m); err != nil {
		return defaultManifest(retentionDays), err
	}
	return m, nil
}

func writeManifest(basePath string, m Manifest) error {
	m.GeneratedAt = time.Now().UTC()
	path := filepath.Join(basePath, "manifest.json")
	tmp := path + ".tmp"
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err //nolint:wrapcheck // error text and type kept as is; wrapping changes behavior
	}
	if err := os.WriteFile(tmp, data, 0o644); err != nil { //nolint:gosec // G306: snapshot mode kept as is; changing it alters file access
		return err //nolint:wrapcheck // error text and type kept as is; wrapping changes behavior
	}
	return os.Rename(tmp, path) //nolint:wrapcheck // error text and type kept as is; wrapping changes behavior
}
