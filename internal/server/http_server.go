package server

import (
	"context"
	"net"
	"net/http"
)

// httpServer abstracts the HTTP server implementation for easier testing.
type httpServer interface {
	ListenAndServe() error
	Shutdown(context.Context) error
	Addr() string
	Handler() http.Handler
}

type netHTTPServer struct {
	srv      *http.Server
	listener net.Listener
}

func (s netHTTPServer) ListenAndServe() error {
	if s.listener != nil {
		return s.srv.Serve(s.listener) //nolint:wrapcheck // thin pass-through; callers see the original error
	}
	return s.srv.ListenAndServe() //nolint:wrapcheck // thin pass-through; callers see the original error
}
func (s netHTTPServer) Shutdown(ctx context.Context) error { return s.srv.Shutdown(ctx) } //nolint:wrapcheck // thin pass-through; callers see the original error
func (s netHTTPServer) Addr() string                       { return s.srv.Addr }
func (s netHTTPServer) Handler() http.Handler              { return s.srv.Handler }
