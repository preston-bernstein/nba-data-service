package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/preston-bernstein/nba-data-service/internal/config"
	"github.com/preston-bernstein/nba-data-service/internal/logging"
	"github.com/preston-bernstein/nba-data-service/internal/server"
)

const appVersion = "dev"

func main() {
	if os.Getenv("SKIP_SERVER_RUN") == "1" {
		return
	}

	cfg := config.Load()
	logger := logging.NewLogger(logging.Config{
		Level:   os.Getenv("LOG_LEVEL"),
		Format:  os.Getenv("LOG_FORMAT"),
		Service: "nba-data-service",
		Version: appVersion,
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := server.New(cfg, logger)
	srv.Run(ctx, stop)
}
