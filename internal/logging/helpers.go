package logging

import "log/slog"

// Info logs an info message when a logger is configured.
func Info(logger *slog.Logger, msg string, args ...any) {
	if logger != nil {
		logger.Info(msg, args...)
	}
}

// Warn logs a warning when a logger is configured.
func Warn(logger *slog.Logger, msg string, args ...any) {
	if logger != nil {
		logger.Warn(msg, args...)
	}
}

// Error logs an error when a logger is configured.
func Error(logger *slog.Logger, msg string, err error, args ...any) {
	if logger == nil {
		return
	}
	if err != nil {
		args = append(args, "error", err)
	}
	logger.Error(msg, args...)
}
