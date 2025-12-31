package http

import (
	nethttp "net/http"

	"nba-data-service/internal/http/handlers"
)

// NewRouter registers HTTP routes on a ServeMux.
func NewRouter(handler *handlers.Handler) nethttp.Handler {
	mux := nethttp.NewServeMux()
	mux.HandleFunc("/health", handler.Health)
	mux.HandleFunc("/ready", handler.Ready)
	mux.HandleFunc("/games", handler.GamesToday)
	mux.HandleFunc("/games/today", handler.GamesToday)
	mux.HandleFunc("/games/", handler.GameByID)
	return mux
}
