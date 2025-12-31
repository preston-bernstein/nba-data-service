package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"nba-data-service/internal/config"
	"nba-data-service/internal/logging"
	"nba-data-service/internal/server"
)

const appVersion = "dev"

func main() {
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
