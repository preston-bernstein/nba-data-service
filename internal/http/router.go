package http

import nethttp "net/http"

// NewRouter registers HTTP routes on a ServeMux.
func NewRouter(handler nethttp.Handler) nethttp.Handler {
	mux := nethttp.NewServeMux()
	mux.Handle("/health", handler)
	mux.Handle("/ready", handler)
	mux.Handle("/games", handler)
	mux.Handle("/games/today", handler)
	mux.Handle("/games/", handler)
	mux.Handle("/teams", handler)
	mux.Handle("/teams/", handler)
	mux.Handle("/players", handler)
	mux.Handle("/players/", handler)
	return mux
}
