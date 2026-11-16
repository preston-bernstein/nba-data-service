package providers

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/logging"
	"github.com/preston-bernstein/nba-data-service/internal/metrics"
)

const (
	defaultRetryAttempts = 3
	defaultBackoff       = 200 * time.Millisecond
)

type backoffFunc func(attempt int) time.Duration

// retryingProvider wraps a GameProvider with retry/backoff behavior.
type retryingProvider struct {
	gameProvider GameProvider
	logger       *slog.Logger
	metrics      *metrics.Recorder
	providerName string
	maxAttempts  int
	backoffFn    backoffFunc
	rng          *rand.Rand
}

// RetryConfig tunes a retrying provider. Zero values select the defaults.
type RetryConfig struct {
	Logger *slog.Logger
	// Metrics may be nil.
	Metrics *metrics.Recorder
	// Name labels logs and metrics; when empty it falls back to the provider type.
	Name string
	// MaxAttempts defaults to defaultRetryAttempts when <= 0.
	MaxAttempts int
	// Backoff defaults to defaultBackoff when <= 0.
	Backoff time.Duration
	// RNG drives the jitter; inject one for deterministic tests.
	RNG *rand.Rand
}

// NewRetryingProvider wraps the given provider with retries, using the defaults for any zero config value.
func NewRetryingProvider(inner GameProvider, cfg RetryConfig) GameProvider {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = defaultRetryAttempts
	}
	if cfg.Backoff <= 0 {
		cfg.Backoff = defaultBackoff
	}
	if cfg.Name == "" {
		cfg.Name = defaultProviderName(inner)
	}
	if cfg.RNG == nil {
		cfg.RNG = newJitterRNG(time.Now().UnixNano())
	}

	return &retryingProvider{
		gameProvider: inner,
		logger:       cfg.Logger,
		metrics:      cfg.Metrics,
		providerName: cfg.Name,
		maxAttempts:  cfg.MaxAttempts,
		backoffFn: func(attempt int) time.Duration {
			return time.Duration(attempt) * cfg.Backoff
		},
		rng: cfg.RNG,
	}
}

func defaultProviderName(inner GameProvider) string {
	if inner == nil {
		return "provider"
	}
	return fmt.Sprintf("%T", inner)
}

// newJitterRNG seeds the retry jitter source. Jitter only spreads out retries; it is not a security value.
func newJitterRNG(seed int64) *rand.Rand {
	return rand.New(rand.NewSource(seed)) //nolint:gosec // G404: retry jitter, not a security value
}

func (r *retryingProvider) FetchGames(ctx context.Context, date string, tz string) ([]games.Game, error) {
	var lastErr error

	for attempt := 1; attempt <= r.maxAttempts; attempt++ {
		start := time.Now()
		gm, err := r.gameProvider.FetchGames(ctx, date, tz)
		r.recordAttempt(time.Since(start), err)

		if err == nil {
			if attempt > 1 {
				r.log(ctx, slog.LevelInfo, "provider fetch succeeded",
					"provider", r.providerName,
					"attempt", attempt,
				)
			}
			return gm, nil
		}
		lastErr = err

		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr //nolint:wrapcheck // context error is returned as is; wrapping changes its message and identity
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
		return ctx.Err() //nolint:wrapcheck // context error is returned as is; wrapping changes its message and identity
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
	providerLogger{logger: logger, provider: r.providerName}.log(ctx, level, msg, args...)
}
