package config

import "time"

// SnapshotSyncConfig controls automatic snapshot backfill/prune behavior.
type SnapshotSyncConfig struct {
	Enabled        bool
	Days           int           // how many past days to maintain
	FutureDays     int           // how many future days to prefetch
	Interval       time.Duration // delay between snapshot fetches
	DailyHourUTC   int           // hour of day (0-23) for daily prune/backfill
	RetentionDays  int           // retention for pruning (games)
	AdminToken     string        // reused for refresh endpoint auth
	SnapshotFolder string        // base path for snapshots
}

func loadSnapshotSync() SnapshotSyncConfig {
	// Default retention covers both past and future windows (with some buffer).
	pastDays := intEnvOrDefault(envSnapshotDays, defaultSnapshotDays)
	futureDays := intEnvOrDefault(envSnapshotFutureDays, defaultSnapshotFutureDays)
	// Retain only the rolling past window (+1 for the crossover day); future snapshots are naturally kept.
	retentionDays := pastDays + 1

	return SnapshotSyncConfig{
		Enabled:        boolEnvOrDefault(envSnapshotSync, defaultSnapshotSync),
		Days:           pastDays,
		FutureDays:     futureDays,
		Interval:       durationEnvOrDefault(envSnapshotRate, defaultSnapshotInterval),
		DailyHourUTC:   intEnvOrDefault(envSnapshotHour, defaultSnapshotDailyHour),
		RetentionDays:  retentionDays,
		AdminToken:     envOrDefault(envAdminToken, ""),
		SnapshotFolder: "data/snapshots",
	}
}
