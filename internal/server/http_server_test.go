package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"
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
	done := make(chan error, 1)
	go func() { done <- s.ListenAndServe() }()

	time.Sleep(50 * time.Millisecond)
	_ = srv.Shutdown(context.Background())

	select {
	case <-done:
		// Any return (success or error) is acceptable; ensure it exits promptly.
	case <-time.After(1 * time.Second):
		t.Fatalf("listen did not return after shutdown")
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
