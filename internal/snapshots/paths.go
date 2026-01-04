package snapshots

import (
	"fmt"
	"path/filepath"
)

// GameSnapshotPath builds the path to a games snapshot for a given date.
func GameSnapshotPath(basePath, date string) string {
	return filepath.Join(basePath, "games", fmt.Sprintf("%s.json", date))
}
