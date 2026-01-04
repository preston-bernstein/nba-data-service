package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
)

type stubListener struct {
	addr net.Addr
}

func (s *stubListener) Accept() (net.Conn, error) { return nil, errors.New("accept failure") }
func (s *stubListener) Close() error              { return nil }
func (s *stubListener) Addr() net.Addr            { return s.addr }

func TestNetHTTPServerListenAndServeWithCustomListener(t *testing.T) {
	l := &stubListener{addr: &net.TCPAddr{IP: net.IPv4zero, Port: 0}}
	srv := &http.Server{Handler: http.NewServeMux()}
	s := netHTTPServer{srv: srv, listener: l}

	if err := s.ListenAndServe(); err == nil {
		t.Fatalf("expected serve error from stub listener")
	}
}

func TestNetHTTPServerListenAndServeWithoutListener(t *testing.T) {
	srv := &http.Server{Addr: "127.0.0.1:0", Handler: http.NewServeMux()}
	s := netHTTPServer{srv: srv}
	if err := s.ListenAndServe(); err == nil {
		t.Fatalf("expected listen error without permissions")
	}
}

func TestNetHTTPServerAccessors(t *testing.T) {
	handler := http.NewServeMux()
	srv := &http.Server{Addr: ":1234", Handler: handler}
	s := netHTTPServer{srv: srv}

	if s.Addr() != ":1234" {
		t.Fatalf("expected addr passthrough")
	}
	if s.Handler() != handler {
		t.Fatalf("expected handler passthrough")
	}
	_ = s.Shutdown(context.Background())
}
