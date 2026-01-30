package poller

import (
	"context"
	"log/slog"
	"sync"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/logging"
	"github.com/preston-bernstein/nba-data-service/internal/metrics"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
	"github.com/preston-bernstein/nba-data-service/internal/timeutil"
)

const defaultInterval = 30 * time.Second

// SnapshotWriter persists game snapshots to disk.
type SnapshotWriter interface {
	WriteGamesSnapshot(date string, snapshot domaingames.TodayResponse) error
}

// Poller fetches games on an interval and writes today's snapshot to disk.
type Poller struct {
	provider providers.GameProvider
	writer   SnapshotWriter
	logger   *slog.Logger
	metrics  *metrics.Recorder
	interval time.Duration
	now      func() time.Time

	ticker   *time.Ticker
	done     chan struct{}
	stopOnce sync.Once
	startMu  sync.Mutex
	started  bool

	statusMu sync.RWMutex
	status   Status
}

// Status describes the recent health of the poller loop.
type Status struct {
	ConsecutiveFailures int
	LastError           string
	LastAttempt         time.Time
	LastSuccess         time.Time
}

// IsReady reports whether the poller has had a recent success and is not failing repeatedly.
func (s Status) IsReady() bool {
	if s.LastSuccess.IsZero() {
		return false
	}
	return s.ConsecutiveFailures < 3
}

// New constructs a Poller with sane defaults.
func New(provider providers.GameProvider, writer SnapshotWriter, logger *slog.Logger, recorder *metrics.Recorder, interval time.Duration) *Poller {
	if interval <= 0 {
		interval = defaultInterval
	}
	return &Poller{
		provider: provider,
		writer:   writer,
		logger:   logger,
		metrics:  recorder,
		interval: interval,
		now:      time.Now,
		done:     make(chan struct{}),
	}
}

// Start begins polling until the context is cancelled or Stop is called.
func (p *Poller) Start(ctx context.Context) {
	p.startMu.Lock()
	if p.started {
		p.startMu.Unlock()
		return
	}
	p.started = true
	p.startMu.Unlock()

	p.ticker = time.NewTicker(p.interval)

	go func() {
		p.logInfo("poller started", slog.Int64(logging.FieldDurationMS, p.interval.Milliseconds()))
		// Initial fetch to warm data on boot.
		p.fetchOnce(ctx)

		for {
			select {
			case <-ctx.Done():
				p.stopTicker()
				p.logInfo("poller stopped")
				return
			case <-p.done:
				p.stopTicker()
				p.logInfo("poller stopped")
				return
			case <-p.ticker.C:
				p.fetchOnce(ctx)
			}
		}
	}()
}

// Stop halts the polling loop.
func (p *Poller) Stop(ctx context.Context) error {
	_ = ctx
	p.stopOnce.Do(func() {
		close(p.done)
		p.stopTicker()
	})
	return nil
}

func (p *Poller) fetchOnce(ctx context.Context) {
	start := time.Now()
	p.recordAttempt(start)
	today := timeutil.FormatDate(p.now().UTC())
	games, err := p.provider.FetchGames(ctx, today, "")
	if p.metrics != nil {
		p.metrics.RecordPollerCycle(time.Since(start), err)
	}
	if err != nil {
		p.logError("poller fetch failed", err, slog.Int64(logging.FieldDurationMS, time.Since(start).Milliseconds()))
		p.recordFailure(err, start)
		return
	}

	if p.writer != nil {
		snap := domaingames.NewTodayResponse(today, games)
		if writeErr := p.writer.WriteGamesSnapshot(today, snap); writeErr != nil {
			p.logError("poller snapshot write failed", writeErr)
		}
	}
	p.recordSuccess(start)
	p.logInfo("poller refreshed games",
		logging.FieldCount, len(games),
		logging.FieldDurationMS, time.Since(start).Milliseconds(),
	)
}

func (p *Poller) stopTicker() {
	if p.ticker != nil {
		p.ticker.Stop()
	}
}

func (p *Poller) logInfo(msg string, args ...any) {
	if p.logger != nil {
		p.logger.Info(msg, args...)
	}
}

func (p *Poller) logError(msg string, err error, attrs ...any) {
	if p.logger != nil {
		p.logger.Error(msg, append(attrs, "error", err)...)
	}
}

func (p *Poller) recordAttempt(at time.Time) {
	p.statusMu.Lock()
	defer p.statusMu.Unlock()
	p.status.LastAttempt = at
}

func (p *Poller) recordSuccess(at time.Time) {
	p.statusMu.Lock()
	defer p.statusMu.Unlock()
	p.status.ConsecutiveFailures = 0
	p.status.LastError = ""
	p.status.LastSuccess = at
}

func (p *Poller) recordFailure(err error, at time.Time) {
	p.statusMu.Lock()
	defer p.statusMu.Unlock()
	p.status.ConsecutiveFailures++
	if err != nil {
		p.status.LastError = err.Error()
	}
	p.status.LastAttempt = at
}

// Status returns a snapshot of the poller's recent health.
func (p *Poller) Status() Status {
	p.statusMu.RLock()
	defer p.statusMu.RUnlock()
	return p.status
}

// Provider exposes the underlying provider (primarily for cleanup in callers).
func (p *Poller) Provider() providers.GameProvider {
	return p.provider
}
