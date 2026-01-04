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
	f, err := os.Open(path)
	if err != nil {
		return defaultManifest(retentionDays), err
	}
	defer f.Close()
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
		return err
	}
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
