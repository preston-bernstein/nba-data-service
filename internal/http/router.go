package http

import nethttp "net/http"

// NewRouter registers HTTP routes on a ServeMux.
func NewRouter(handler *Handler) nethttp.Handler {
	mux := nethttp.NewServeMux()
	mux.HandleFunc("/health", handler.Health)
	mux.HandleFunc("/ready", handler.Ready)
	mux.HandleFunc("/games", handler.GamesToday)
	mux.HandleFunc("/games/today", handler.GamesToday)
	mux.HandleFunc("/games/", handler.GameByID)
	return mux
}
