package providers

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"nba-data-service/internal/domain"
	"nba-data-service/internal/logging"
	"nba-data-service/internal/metrics"
)

const (
	defaultRetryAttempts = 3
	defaultBackoff       = 200 * time.Millisecond
)

type backoffFunc func(attempt int) time.Duration

// retryingProvider wraps a GameProvider with retry/backoff behavior.
type retryingProvider struct {
	inner        GameProvider
	logger       *slog.Logger
	metrics      *metrics.Recorder
	providerName string
	maxAttempts  int
	backoffFn    backoffFunc
	rng          *rand.Rand
}

// NewRetryingProvider wraps the given provider with retries. If maxAttempts/backoff are <= 0, defaults are used.
// providerName is optional; when empty it falls back to the provider type.
func NewRetryingProvider(inner GameProvider, logger *slog.Logger, metricsRecorder *metrics.Recorder, providerName string, maxAttempts int, backoff time.Duration) GameProvider {
	return NewRetryingProviderWithRNG(inner, logger, metricsRecorder, providerName, nil, maxAttempts, backoff)
}

// NewRetryingProviderWithRNG is identical to NewRetryingProvider but allows injecting a rand.Rand for deterministic tests.
func NewRetryingProviderWithRNG(inner GameProvider, logger *slog.Logger, metricsRecorder *metrics.Recorder, providerName string, rng *rand.Rand, maxAttempts int, backoff time.Duration) GameProvider {
	if maxAttempts <= 0 {
		maxAttempts = defaultRetryAttempts
	}
	if backoff <= 0 {
		backoff = defaultBackoff
	}

	if providerName == "" {
		if inner != nil {
			providerName = fmt.Sprintf("%T", inner)
		} else {
			providerName = "provider"
		}
	}

	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	return &retryingProvider{
		inner:        inner,
		logger:       logger,
		metrics:      metricsRecorder,
		providerName: providerName,
		maxAttempts:  maxAttempts,
		backoffFn: func(attempt int) time.Duration {
			return time.Duration(attempt) * backoff
		},
		rng: rng,
	}
}

func (r *retryingProvider) FetchGames(ctx context.Context, date string, tz string) ([]domain.Game, error) {
	var lastErr error

	for attempt := 1; attempt <= r.maxAttempts; attempt++ {
		start := time.Now()
		games, err := r.inner.FetchGames(ctx, date, tz)
		r.recordAttempt(time.Since(start), err)

		if err == nil {
			if attempt > 1 {
				r.log(ctx, slog.LevelInfo, "provider fetch succeeded",
					"provider", r.providerName,
					"attempt", attempt,
				)
			}
			return games, nil
		}
		lastErr = err

		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}

		if attempt == r.maxAttempts {
			break
		}

		delay := r.computeDelay(err, attempt)
		r.logRetry(ctx, attempt, delay, err)
		if sleepErr := r.sleep(ctx, delay); sleepErr != nil {
			return nil, sleepErr
		}
	}

	r.log(ctx, slog.LevelWarn, "provider fetch failed", "provider", r.providerName, "attempts", r.maxAttempts, "err", lastErr)
	return nil, lastErr
}

func (r *retryingProvider) computeDelay(err error, attempt int) time.Duration {
	base := r.backoffFn(attempt)

	if rlErr, ok := AsRateLimitError(err); ok {
		if r.metrics != nil {
			r.metrics.RecordRateLimit(r.providerName, rlErr.RetryAfter)
		}
		if rlErr.RetryAfter > 0 {
			return rlErr.RetryAfter
		}
	}

	return r.jitterDelay(base)
}

func (r *retryingProvider) jitterDelay(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}

	half := base / 2
	if half <= 0 {
		return base
	}

	return half + time.Duration(r.rng.Int63n(int64(half)+1))
}

func (r *retryingProvider) sleep(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *retryingProvider) recordAttempt(duration time.Duration, err error) {
	if r.metrics != nil {
		r.metrics.RecordProviderAttempt(r.providerName, duration, err)
	}
}

func (r *retryingProvider) logRetry(ctx context.Context, attempt int, delay time.Duration, err error) {
	args := []any{
		"provider", r.providerName,
		"attempt", attempt,
		"max_attempts", r.maxAttempts,
		"backoff_ms", delay.Milliseconds(),
		"err", err,
	}

	if rlErr, ok := AsRateLimitError(err); ok {
		args = append(args,
			"status_code", rlErr.StatusCode,
		)
		if rlErr.RetryAfter > 0 {
			args = append(args, "retry_after_ms", rlErr.RetryAfter.Milliseconds())
		}
		if rlErr.Remaining != "" {
			args = append(args, "rate_limit_remaining", rlErr.Remaining)
		}
	}

	r.log(ctx, slog.LevelWarn, "provider fetch retry", args...)
}

func (r *retryingProvider) log(ctx context.Context, level slog.Level, msg string, args ...any) {
	logger := logging.FromContext(ctx, r.logger)
	logWithProvider(ctx, logger, level, r.providerName, msg, args...)
}
