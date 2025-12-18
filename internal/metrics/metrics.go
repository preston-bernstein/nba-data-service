package metrics

import (
	"sync"
	"time"
)

type providerStats struct {
	calls           int
	errors          int
	rateLimitHits   int
	lastRetryAfter  time.Duration
	lastCallLatency time.Duration
}

// Recorder captures lightweight, in-memory metrics about provider calls.
// It is intentionally simple so it can be swapped for a real backend later.
type Recorder struct {
	mu    sync.Mutex
	stats map[string]*providerStats
	otel  *otelInstruments
}

func NewRecorder() *Recorder {
	return newRecorder(nil)
}

func newRecorder(otel *otelInstruments) *Recorder {
	return &Recorder{
		stats: make(map[string]*providerStats),
		otel:  otel,
	}
}

// RecordProviderAttempt increments counters for a provider call and stores the last observed latency.
func (r *Recorder) RecordProviderAttempt(provider string, duration time.Duration, err error) {
	if r == nil {
		return
	}

	stats := r.ensureStats(provider)
	stats.calls++
	stats.lastCallLatency = duration
	if err != nil {
		stats.errors++
	}
	if r.otel != nil {
		r.otel.recordProviderAttempt(provider, duration, err)
	}
}

// RecordRateLimit tracks that a provider response hit a rate limit and stores the last Retry-After.
func (r *Recorder) RecordRateLimit(provider string, retryAfter time.Duration) {
	if r == nil {
		return
	}

	stats := r.ensureStats(provider)
	stats.rateLimitHits++
	if retryAfter > 0 {
		stats.lastRetryAfter = retryAfter
	}
	if r.otel != nil {
		r.otel.recordRateLimit(provider, retryAfter)
	}
}

// ProviderCalls returns the total attempts recorded for a provider.
func (r *Recorder) ProviderCalls(provider string) int {
	return r.Snapshot(provider).Calls
}

// ProviderErrors returns the total failed attempts recorded for a provider.
func (r *Recorder) ProviderErrors(provider string) int {
	return r.Snapshot(provider).Errors
}

// RateLimitHits returns the number of rate limit events seen for a provider.
func (r *Recorder) RateLimitHits(provider string) int {
	return r.Snapshot(provider).RateLimitHits
}

// LastRetryAfter returns the most recent Retry-After recorded for a provider.
func (r *Recorder) LastRetryAfter(provider string) time.Duration {
	return r.Snapshot(provider).LastRetryAfter
}

// LastCallLatency returns the last recorded latency for a provider call.
func (r *Recorder) LastCallLatency(provider string) time.Duration {
	return r.Snapshot(provider).LastCallLatency
}

// Snapshot returns a copy of the current stats for the provider.
type Snapshot struct {
	Calls           int
	Errors          int
	RateLimitHits   int
	LastRetryAfter  time.Duration
	LastCallLatency time.Duration
}

func (r *Recorder) Snapshot(provider string) Snapshot {
	if r == nil {
		return Snapshot{}
	}
	stats := r.snapshot(provider)
	return Snapshot{
		Calls:           stats.calls,
		Errors:          stats.errors,
		RateLimitHits:   stats.rateLimitHits,
		LastRetryAfter:  stats.lastRetryAfter,
		LastCallLatency: stats.lastCallLatency,
	}
}

// RecordHTTPRequest tracks basic HTTP metrics.
func (r *Recorder) RecordHTTPRequest(method, path string, status int, duration time.Duration) {
	if r == nil || r.otel == nil {
		return
	}
	r.otel.recordHTTPRequest(method, path, status, duration)
}

// RecordPollerCycle tracks poller cycles and errors.
func (r *Recorder) RecordPollerCycle(duration time.Duration, err error) {
	if r == nil || r.otel == nil {
		return
	}
	r.otel.recordPoller(duration, err)
}

func (r *Recorder) ensureStats(provider string) *providerStats {
	r.mu.Lock()
	defer r.mu.Unlock()

	stats, ok := r.stats[provider]
	if !ok {
		stats = &providerStats{}
		r.stats[provider] = stats
	}
	return stats
}

func (r *Recorder) snapshot(provider string) providerStats {
	r.mu.Lock()
	defer r.mu.Unlock()

	if stats, ok := r.stats[provider]; ok && stats != nil {
		return *stats
	}
	return providerStats{}
}
