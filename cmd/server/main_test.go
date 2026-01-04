package main

import (
	"testing"
)

// Smoke test to ensure main honors SKIP_SERVER_RUN and does not block test runs.
func TestMainSkipsWhenEnvSet(t *testing.T) {
	t.Setenv("SKIP_SERVER_RUN", "1")
	main()
}
