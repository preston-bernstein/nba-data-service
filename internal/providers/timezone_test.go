package providers

import "testing"

func TestResolveTimezoneValid(t *testing.T) {
	loc := ResolveTimezone("UTC")
	if loc == nil || loc.String() != "UTC" {
		t.Fatalf("expected UTC, got %v", loc)
	}
}

func TestResolveTimezoneInvalid(t *testing.T) {
	if loc := ResolveTimezone("Not/AZone"); loc != nil {
		t.Fatalf("expected nil for invalid timezone, got %v", loc)
	}
}

func TestResolveTimezoneEmpty(t *testing.T) {
	if loc := ResolveTimezone(""); loc != nil {
		t.Fatalf("expected nil for empty timezone")
	}
}
