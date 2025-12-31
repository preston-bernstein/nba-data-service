package server

import (
	"context"

	"nba-data-service/internal/poller"
)

// Poller defines the minimal poller behavior needed by the server.
type Poller interface {
	Start(ctx context.Context)
	Stop(ctx context.Context) error
	Status() poller.Status
}
