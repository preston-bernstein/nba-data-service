package metrics

import (
	"context"
	"testing"
)

func TestSetupDisabledReturnsNoHandler(t *testing.T) {
	rec, handler, shutdown, err := Setup(context.Background(), TelemetryConfig{
		Enabled: false,
	})
	if err != nil {
		t.Fatalf("expected no error when disabled, got %v", err)
	}
	if rec == nil {
		t.Fatalf("expected recorder")
	}
	if handler != nil {
		t.Fatalf("expected nil handler when disabled")
	}
	if shutdown == nil {
		t.Fatalf("expected shutdown function")
	}
}
