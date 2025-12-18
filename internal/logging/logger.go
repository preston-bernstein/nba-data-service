package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type Config struct {
	Level   string
	Format  string
	Service string
	Version string
}

const (
	formatJSON    = "json"
	formatText    = "text"
	defaultLevel  = "info"
	defaultFormat = formatJSON
)

// NewLogger returns a structured logger with sane defaults.
func NewLogger(cfg Config) *slog.Logger {
	level := parseLevel(cfg.Level)
	handler := buildHandler(cfg.Format, level)

	return slog.New(handler).With(
		slog.String("service", cfg.Service),
		slog.String("version", cfg.Version),
	)
}

// FromContext returns a logger from context or a fallback.
func FromContext(ctx context.Context, fallback *slog.Logger) *slog.Logger {
	if ctx == nil {
		return fallback
	}
	if logger, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok && logger != nil {
		return logger
	}
	return fallback
}

type loggerKey struct{}

// WithLogger stores a logger in context.
func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

func parseLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func buildHandler(format string, level slog.Level) slog.Handler {
	opts := &slog.HandlerOptions{Level: level}
	if strings.ToLower(strings.TrimSpace(format)) == formatText {
		opts.AddSource = true // helpful for local debugging
		return slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.NewJSONHandler(os.Stdout, opts)
}
