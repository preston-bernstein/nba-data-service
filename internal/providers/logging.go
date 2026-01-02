package providers

import (
	"context"
	"log/slog"
)

// logWithProvider emits a log entry if logger is non-nil and always includes provider name.
func logWithProvider(ctx context.Context, logger *slog.Logger, level slog.Level, provider string, msg string, args ...any) {
	if logger == nil {
		return
	}
	args = append(args, slog.String("provider", provider))
	logger.Log(ctx, level, msg, args...)
}
