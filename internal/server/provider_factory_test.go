package server

import (
	"testing"

	"nba-data-service/internal/config"
)

func TestProviderFactoryBuildsWithDefaultInterval(t *testing.T) {
	factory := newProviderFactory(nil, nil)
	prov := factory.build(config.Config{Provider: "fixture"})
	if prov == nil {
		t.Fatalf("expected provider")
	}
}
