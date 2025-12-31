package server

import "time"

const (
	readTimeout  = 10 * time.Second
	writeTimeout = 10 * time.Second
	idleTimeout  = 60 * time.Second
)

// shutdownTimeout remains a var for tests to override.
var shutdownTimeout = 10 * time.Second
