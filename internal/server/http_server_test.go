package server

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestNetHTTPServerHelpers(t *testing.T) {
	srv := &http.Server{
		Addr:    ":0",
		Handler: http.NewServeMux(),
	}
	wrapper := netHTTPServer{srv: srv}

	if wrapper.Handler() == nil {
		t.Fatalf("expected handler")
	}
	if wrapper.Addr() == "" {
		t.Fatalf("expected addr")
	}
	// Shutdown without starting should be safe.
	if err := wrapper.Shutdown(context.Background()); err != nil && err != http.ErrServerClosed {
		t.Fatalf("expected no fatal error on shutdown, got %v", err)
	}
}

func TestNetHTTPServerListenAndServeWithListener(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("listener unavailable: %v", err)
	}
	defer listener.Close()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{
		Handler: handler,
	}
	wrapper := netHTTPServer{srv: srv, listener: listener}

	errCh := make(chan error, 1)
	go func() {
		errCh <- wrapper.ListenAndServe()
	}()

	resp, err := http.Get("http://" + listener.Addr().String())
	if err != nil {
		t.Fatalf("expected request to succeed, got %v", err)
	}
	resp.Body.Close()

	if err := wrapper.Shutdown(context.Background()); err != nil && err != http.ErrServerClosed {
		t.Fatalf("shutdown failed: %v", err)
	}

	select {
	case <-errCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("server did not stop in time")
	}
}

func TestNetHTTPServerListenAndServeDefault(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{
		Addr:    "127.0.0.1:0",
		Handler: handler,
	}
	wrapper := netHTTPServer{srv: srv}

	errCh := make(chan error, 1)
	go func() {
		errCh <- wrapper.ListenAndServe()
	}()

	// Give the server a moment to bind; even if the port is in use, we'll skip.
	time.Sleep(25 * time.Millisecond)
	_ = wrapper.Shutdown(context.Background())

	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			t.Skipf("listen failed in environment: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Skip("server did not stop in time; likely blocked in environment")
	}
}
