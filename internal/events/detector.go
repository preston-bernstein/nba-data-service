package events

import (
	"sync"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
)

type seenState struct {
	statusKind domaingames.GameStatusKind
	score      domaingames.Score
}

// Detector remembers the last published statusKind and score per game id.
// State lives in memory, so the first poll after a restart reports every game as created.
type Detector struct {
	mu   sync.Mutex
	seen map[string]seenState
}

// NewDetector returns an empty Detector.
func NewDetector() *Detector {
	return &Detector{seen: make(map[string]seenState)}
}

// Check reports whether the game needs an event and why.
// It does not change state. Call Commit after the event is published,
// so a failed publish is tried again on the next poll.
func (d *Detector) Check(g domaingames.Game) (ChangeKind, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	prev, ok := d.seen[g.ID]
	switch {
	case !ok:
		return ChangeCreated, true
	case prev.statusKind != g.StatusKind:
		return ChangeStatus, true
	case prev.score != g.Score:
		return ChangeScore, true
	}
	return "", false
}

// Commit records the game as published.
func (d *Detector) Commit(g domaingames.Game) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seen[g.ID] = seenState{statusKind: g.StatusKind, score: g.Score}
}
