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
	rec.RecordPollerCycle(time.Second, errors.New("fail"))
	rec.RecordHTTPRequest("GET", "/health", 200, time.Millisecond)

	if got := rec.RateLimitHits("balldontlie"); got != 2 {
		t.Fatalf("expected 2 rate limit hits, got %d", got)
	}
	if got := rec.LastRetryAfter("balldontlie"); got != 5*time.Second {
		t.Fatalf("expected last retry-after to be 5s, got %s", got)
	}
}

func TestRecorderNilSafeSnapshotAndRecords(t *testing.T) {
	var rec *Recorder
	snap := rec.Snapshot("missing")
	if snap.Calls != 0 || snap.Errors != 0 || snap.RateLimitHits != 0 {
		t.Fatalf("expected zero snapshot for nil recorder, got %+v", snap)
	}
	// Ensure nil recorder does not panic on recorders without otel.
	rec.RecordProviderAttempt("p", time.Millisecond, errors.New("err"))
	rec.RecordRateLimit("p", time.Second)
	rec.RecordHTTPRequest("GET", "/health", 200, time.Millisecond)
	rec.RecordPollerCycle(time.Millisecond, nil)
}

func TestRecorderSnapshotMissingProviderReturnsZero(t *testing.T) {
	rec := NewRecorder()
	snap := rec.Snapshot("unknown")
	if snap.Calls != 0 || snap.Errors != 0 || snap.RateLimitHits != 0 {
		t.Fatalf("expected zero snapshot, got %+v", snap)
	}
}
