package server

import (
	"context"
	"log/slog"

	"nba-data-service/internal/config"
	"nba-data-service/internal/providers"
	"nba-data-service/internal/snapshots"
)

type snapshotComponents struct {
	store  snapshots.Store
	writer *snapshots.Writer
	syncer *snapshots.Syncer
}

func buildSnapshots(cfg config.Config, provider providers.GameProvider, logger *slog.Logger) snapshotComponents {
	basePath := cfg.Snapshots.SnapshotFolder
	writer := snapshots.NewWriter(basePath, cfg.Snapshots.RetentionDays)
	store := snapshots.NewFSStore(basePath)
	syncer := snapshots.NewSyncer(provider, writer, snapshots.SyncConfig{
		Enabled:      cfg.Snapshots.Enabled,
		Days:         cfg.Snapshots.Days,
		FutureDays:   cfg.Snapshots.FutureDays,
		Interval:     cfg.Snapshots.Interval,
		DailyHourUTC: cfg.Snapshots.DailyHourUTC,
	}, logger)
	go syncer.Run(context.Background())

	return snapshotComponents{
		store:  store,
		writer: writer,
		syncer: syncer,
	}
}
