package server

import (
	"context"
	"testing"

	"github.com/preston-bernstein/nba-data-service/internal/config"
	"github.com/preston-bernstein/nba-data-service/internal/providers/fixture"
)

func TestBuildSnapshotsRespectsConfig(t *testing.T) {
	cfg := config.Config{
		Snapshots: config.SnapshotSyncConfig{
			Enabled:        false, // disable background goroutine in test
			RetentionDays:  1,
			SnapshotFolder: t.TempDir(),
		},
	}
	prov := fixture.New()
	components := buildSnapshots(cfg, prov, nil)
	if components.store == nil || components.writer == nil || components.syncer == nil {
		t.Fatalf("expected snapshots components to be initialized")
	}
	// Ensure syncer can be stopped quickly.
	cancel, cancelFn := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		components.syncer.Run(cancel)
		close(done)
	}()
	cancelFn()
	<-done
}
