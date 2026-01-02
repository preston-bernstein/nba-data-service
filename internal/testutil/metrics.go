package testutil

import (
	"context"

	"nba-data-service/internal/metrics"
)

// NewRecorderWithShutdown returns a recorder and a no-op shutdown to simplify tests.
func NewRecorderWithShutdown() (*metrics.Recorder, func(context.Context) error) {
	return metrics.NewRecorder(), func(context.Context) error { return nil }
}
