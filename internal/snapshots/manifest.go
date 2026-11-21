package snapshots

import (
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
	var m Manifest
	if err := readJSONFile(path, &m); err != nil {
		return defaultManifest(retentionDays), err
	}
	return m, nil
}

func writeManifest(basePath string, m Manifest) error {
	m.GeneratedAt = time.Now().UTC()
	data, err := encodeJSON(m)
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Join(basePath, "manifest.json"), data)
}
