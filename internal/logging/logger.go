package logging

import (
	"log/slog"
	"os"
)

// NewLogger returns a structured logger with sane defaults.
func NewLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
}
