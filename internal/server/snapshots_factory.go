package server

import (
	"context"
	"log/slog"

	"github.com/preston-bernstein/nba-data-service/internal/config"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
	"github.com/preston-bernstein/nba-data-service/internal/snapshots"
)

type snapshotComponents struct {
	store  snapshots.Store
	writer *snapshots.Writer
	syncer *snapshots.Syncer
}

func buildSnapshots(cfg config.Config, provider providers.GameProvider, roster snapshots.RosterStore, logger *slog.Logger) snapshotComponents {
	basePath := cfg.Snapshots.SnapshotFolder
	writer := snapshots.NewWriter(basePath, cfg.Snapshots.RetentionDays)
	store := snapshots.NewFSStore(basePath)

	dataProvider, _ := provider.(providers.DataProvider)
	syncer := snapshots.NewSyncer(dataProvider, writer, snapshots.SyncConfig{
		Enabled:      cfg.Snapshots.Enabled,
		Days:         cfg.Snapshots.Days,
		FutureDays:   cfg.Snapshots.FutureDays,
		Interval:     cfg.Snapshots.Interval,
		DailyHourUTC: cfg.Snapshots.DailyHourUTC,
		TeamsRefreshDays:    cfg.Snapshots.TeamsRefreshDays,
		PlayersRefreshHours: cfg.Snapshots.PlayersRefreshHours,
	}, logger, roster)
	if cfg.Snapshots.Enabled {
		go syncer.Run(context.Background())
	}

	return snapshotComponents{
		store:  store,
		writer: writer,
		syncer: syncer,
	}
}
