package timeutil

import (
	"testing"
	"time"
)

func TestParseDate(t *testing.T) {
	parsed, err := ParseDate("2024-01-02")
	if err != nil {
		t.Fatalf("expected parse to succeed, got %v", err)
	}
	if got := FormatDate(parsed); got != "2024-01-02" {
		t.Fatalf("expected formatted date to round-trip, got %s", got)
	}
}

func TestFormatDateUsesLocation(t *testing.T) {
	loc := time.FixedZone("test", -5*60*60)
	value := time.Date(2024, 1, 2, 23, 0, 0, 0, loc)
	if got := FormatDate(value); got != "2024-01-02" {
		t.Fatalf("expected formatted date, got %s", got)
	}
}
