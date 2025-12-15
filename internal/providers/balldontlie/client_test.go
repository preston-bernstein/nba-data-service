package balldontlie

import (
	"context"
	"testing"
)

func TestNewClientNotNil(t *testing.T) {
	if NewClient() == nil {
		t.Fatal("expected client to be non-nil")
	}
}

func TestFetchGamesStubReturnsEmpty(t *testing.T) {
	c := NewClient()
	games, err := c.FetchGames(context.Background())
	if err != nil {
		t.Fatalf("expected no error from stub fetch, got %v", err)
	}
	if len(games) != 0 {
		t.Fatalf("expected empty games from stub, got %d", len(games))
	}
}
