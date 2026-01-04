package logging

import (
	"context"
	"log/slog"
	"testing"
)

func TestFromContextAndWithLogger(t *testing.T) {
	base := slog.Default()
	ctx := context.Background()
	if got := FromContext(ctx, base); got != base {
		t.Fatalf("expected fallback logger")
	}

	ctx = WithLogger(ctx, nil)
	if got := FromContext(ctx, base); got != base {
		t.Fatalf("nil logger should fall back")
	}

	custom := slog.Default().With("k", "v")
	ctx = WithLogger(ctx, custom)
	if got := FromContext(ctx, base); got != custom {
		t.Fatalf("expected custom logger from context")
	}
}

func TestParseLevel(t *testing.T) {
	if parseLevel("debug") != slog.LevelDebug {
		t.Fatalf("expected debug level")
	}
	if parseLevel("warn") != slog.LevelWarn {
		t.Fatalf("expected warn level")
	}
	if parseLevel("error") != slog.LevelError {
		t.Fatalf("expected error level")
	}
	if parseLevel("") != slog.LevelInfo {
		t.Fatalf("expected default info level")
	}
}

func TestBuildHandlerFormats(t *testing.T) {
	jsonHandler := buildHandler("json", slog.LevelInfo)
	if jsonHandler == nil {
		t.Fatalf("expected json handler")
	}
	textHandler := buildHandler("text", slog.LevelDebug)
	if textHandler == nil {
		t.Fatalf("expected text handler")
	}
}

func TestNewLoggerAddsFields(t *testing.T) {
	logger := NewLogger(Config{
		Service: "svc",
		Version: "v1",
	})
	if logger == nil {
		t.Fatalf("expected logger")
	}
}

func TestBuildHandlerReturnsJSONAndText(t *testing.T) {
	if _, ok := buildHandler("json", slog.LevelInfo).(*slog.JSONHandler); !ok {
		t.Fatalf("expected JSON handler")
	}
	if _, ok := buildHandler("text", slog.LevelWarn).(*slog.TextHandler); !ok {
		t.Fatalf("expected text handler")
	}
}

func TestParseLevelFallsBackOnUnknown(t *testing.T) {
	if got := parseLevel("DEBUG"); got != slog.LevelDebug {
		t.Fatalf("expected debug for DEBUG")
	}
	if got := parseLevel("unknown"); got != slog.LevelInfo {
		t.Fatalf("expected info fallback, got %v", got)
	}
}

func TestFromContextHandlesNilContext(t *testing.T) {
	logger := slog.Default()
	if got := FromContext(nil, logger); got != logger {
		t.Fatalf("expected fallback logger for nil context")
	}
}
