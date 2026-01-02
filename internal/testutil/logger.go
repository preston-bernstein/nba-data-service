package testutil

import (
	"bytes"
	"log/slog"
)

// NewBufferLogger returns a slog logger backed by a buffer and the buffer for assertions.
func NewBufferLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	return logger, &buf
}
