package metrics

import (
	"errors"
	"testing"
	"time"
)

func TestRecorderTracksProviderAttemptsAndErrors(t *testing.T) {
	rec := NewRecorder()
	rec.RecordProviderAttempt("balldontlie", 10*time.Millisecond, nil)
	rec.RecordProviderAttempt("balldontlie", 15*time.Millisecond, errors.New("boom"))

	if got := rec.ProviderCalls("balldontlie"); got != 2 {
		t.Fatalf("expected 2 calls, got %d", got)
	}
	if got := rec.ProviderErrors("balldontlie"); got != 1 {
		t.Fatalf("expected 1 error, got %d", got)
	}
	if got := rec.LastCallLatency("balldontlie"); got != 15*time.Millisecond {
		t.Fatalf("expected last latency to be 15ms, got %s", got)
	}

	snap := rec.Snapshot("balldontlie")
	if snap.Calls != 2 || snap.Errors != 1 {
		t.Fatalf("unexpected snapshot %+v", snap)
	}
}

func TestRecorderTracksRateLimits(t *testing.T) {
	rec := NewRecorder()
	rec.RecordRateLimit("balldontlie", 5*time.Second)
	rec.RecordRateLimit("balldontlie", 0)

	if got := rec.RateLimitHits("balldontlie"); got != 2 {
		t.Fatalf("expected 2 rate limit hits, got %d", got)
	}
	if got := rec.LastRetryAfter("balldontlie"); got != 5*time.Second {
		t.Fatalf("expected last retry-after to be 5s, got %s", got)
	}
}
