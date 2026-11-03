package events

import (
	"testing"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
)

func TestDetectorNewGameIsCreated(t *testing.T) {
	d := NewDetector()
	kind, changed := d.Check(sampleGame())
	if !changed || kind != ChangeCreated {
		t.Fatalf("got %s %v", kind, changed)
	}
}

func TestDetectorUnchangedAfterCommit(t *testing.T) {
	d := NewDetector()
	g := sampleGame()
	d.Commit(g)
	if _, changed := d.Check(g); changed {
		t.Fatal("expected no change")
	}
}

func TestDetectorWithoutCommitStaysChanged(t *testing.T) {
	d := NewDetector()
	g := sampleGame()
	d.Check(g)
	if _, changed := d.Check(g); !changed {
		t.Fatal("uncommitted game must still count as changed")
	}
}

func TestDetectorScoreChange(t *testing.T) {
	d := NewDetector()
	g := sampleGame()
	d.Commit(g)
	g.Score.Home++
	kind, changed := d.Check(g)
	if !changed || kind != ChangeScore {
		t.Fatalf("got %s %v", kind, changed)
	}
}

func TestDetectorStatusChange(t *testing.T) {
	d := NewDetector()
	g := sampleGame()
	d.Commit(g)
	g.StatusKind = domaingames.StatusFinal
	kind, changed := d.Check(g)
	if !changed || kind != ChangeStatus {
		t.Fatalf("got %s %v", kind, changed)
	}
}

func TestDetectorStatusWinsWhenBothChange(t *testing.T) {
	d := NewDetector()
	g := sampleGame()
	d.Commit(g)
	g.StatusKind = domaingames.StatusFinal
	g.Score.Away += 3
	if kind, _ := d.Check(g); kind != ChangeStatus {
		t.Fatalf("got %s", kind)
	}
}

func TestDetectorIgnoresOtherFields(t *testing.T) {
	d := NewDetector()
	g := sampleGame()
	d.Commit(g)
	g.Status = "Q1 5:00"
	g.Meta.Time = "5:00"
	if _, changed := d.Check(g); changed {
		t.Fatal("status text and meta changes must not publish")
	}
}
