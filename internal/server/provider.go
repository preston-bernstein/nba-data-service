package server

import (
	"log/slog"

	"github.com/preston-bernstein/nba-data-service/internal/config"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
	"github.com/preston-bernstein/nba-data-service/internal/providers/balldontlie"
	"github.com/preston-bernstein/nba-data-service/internal/providers/fixture"
)

func selectProvider(cfg config.Config, logger *slog.Logger) providers.GameProvider {
	switch cfg.Provider {
	case "fixture", "":
		return fixture.New()
	case "balldontlie":
		return balldontlie.NewClient(balldontlie.Config{
			BaseURL:  cfg.Balldontlie.BaseURL,
			APIKey:   cfg.Balldontlie.APIKey,
			Timezone: cfg.Balldontlie.Timezone,
			MaxPages: cfg.Balldontlie.MaxPages,
		})
	default:
		if logger != nil {
			logger.Warn("unknown provider, falling back to fixture", slog.String("provider", cfg.Provider))
		}
		return fixture.New()
	}
}
