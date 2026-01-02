package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
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

func TestRecorderNilSafeOtelPaths(t *testing.T) {
	r := NewRecorder()
	// Ensure otel-less recorder does not panic.
	r.RecordHTTPRequest("GET", "/ready", 200, time.Millisecond)
	r.RecordPollerCycle(time.Millisecond, nil)
	r.RecordRateLimit("fixture", 0)
}

func TestSnapshotZeroWhenNoProviderStats(t *testing.T) {
	r := NewRecorder()
	snap := r.Snapshot("none")
	if snap.Calls != 0 || snap.Errors != 0 || snap.RateLimitHits != 0 {
		t.Fatalf("expected zero snapshot, got %+v", snap)
	}
}

func TestRecorderWithOtelInstruments(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	inst, err := newOtelInstruments(provider)
	if err != nil {
		t.Fatalf("expected otel instruments, got %v", err)
	}
	rec := newRecorder(inst)
	rec.RecordHTTPRequest("GET", "/health", 200, time.Millisecond)
	rec.RecordProviderAttempt("fixture", 2*time.Millisecond, nil)
	rec.RecordRateLimit("fixture", time.Second)
	rec.RecordPollerCycle(time.Millisecond, errors.New("fail"))
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

func TestRecorderRateLimitRecordsRetryAfter(t *testing.T) {
	r := NewRecorder()
	r.RecordRateLimit("p", 2*time.Second)
	if hits := r.RateLimitHits("p"); hits != 1 {
		t.Fatalf("expected 1 rate limit hit, got %d", hits)
	}
	if got := r.LastRetryAfter("p"); got != 2*time.Second {
		t.Fatalf("expected retry after recorded, got %v", got)
	}
}

func TestRecorderPollerCycleRecordsError(t *testing.T) {
	r := NewRecorder()
	r.RecordPollerCycle(time.Millisecond, context.DeadlineExceeded)
	_ = r.Snapshot("poller")
}

func TestRecordCounterAndHistogram(t *testing.T) {
	attrs := []attribute.KeyValue{attribute.String("k", "v")}

	// nil receiver is a no-op
	var nilInst *otelInstruments
	nilInst.recordCounter(nil, 1, attrs...)
	nilInst.recordHistogram(nil, 1, attrs...)

	// non-nil path records counters and histograms with attributes.
	inst := &otelInstruments{ctx: context.Background()}
	meter := noop.NewMeterProvider().Meter("test")
	counter, _ := meter.Int64Counter("c")
	hist, _ := meter.Float64Histogram("h")
	inst.recordCounter(counter, 3, attrs...)
	inst.recordHistogram(hist, 5.5, attrs...)
}
