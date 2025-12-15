package logging

import (
	"context"
	"log/slog"
	"testing"
)

func TestNewLoggerNotNil(t *testing.T) {
	logger := NewLogger()
	if logger == nil {
		t.Fatal("expected logger to be non-nil")
	}
}

func TestNewLoggerUsesTextHandlerWithInfoLevel(t *testing.T) {
	logger := NewLogger()

	if enabled := logger.Enabled(context.Background(), slog.LevelInfo); !enabled {
		t.Fatal("expected info level to be enabled")
	}

	if enabled := logger.Enabled(context.Background(), slog.LevelDebug); enabled {
		t.Fatal("expected debug level to be disabled")
	}
}
