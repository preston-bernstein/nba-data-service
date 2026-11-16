package providers

import (
	"context"
	"log/slog"
)

// providerLogger logs through a logger that may be nil and tags every entry with the provider name.
type providerLogger struct {
	logger   *slog.Logger
	provider string
}

// log emits a log entry if the logger is non-nil and always includes the provider name.
func (l providerLogger) log(ctx context.Context, level slog.Level, msg string, args ...any) {
	if l.logger == nil {
		return
	}
	args = append(args, slog.String("provider", l.provider))
	l.logger.Log(ctx, level, msg, args...)
}
