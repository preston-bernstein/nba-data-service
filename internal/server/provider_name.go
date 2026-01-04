package server

import (
	"fmt"
	"strings"

	"github.com/preston-bernstein/nba-data-service/internal/providers"
)

// normalizeProviderName returns a lower-cased provider name, deriving from instance when not explicitly configured.
// Used across server wiring and provider factory to keep naming consistent in metrics/logs.
func normalizeProviderName(raw string, provider providers.GameProvider) string {
	if raw != "" {
		return strings.ToLower(raw)
	}
	if provider != nil {
		return strings.ToLower(fmt.Sprintf("%T", provider))
	}
	return "provider"
}
