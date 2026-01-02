package testutil

import (
	"context"
	"errors"
	"net/http"

	"nba-data-service/internal/poller"
)

// StubPoller implements Poller for tests.
type StubPoller struct {
	StartCalls int
	StopCalls  int
	Err        error
	StatusVal  poller.Status
}

func (p *StubPoller) Start(ctx context.Context) {
	_ = ctx
	p.StartCalls++
}

func (p *StubPoller) Stop(ctx context.Context) error {
	_ = ctx
	p.StopCalls++
	return p.Err
}

func (p *StubPoller) Status() poller.Status {
	return p.StatusVal
}

// StubHTTPServer implements httpServer for tests.
type StubHTTPServer struct {
	AddrVal       string
	HandlerVal    http.Handler
	ListenCalls   int
	ShutdownCalls int
	ListenErr     error
	ShutdownErr   error
}

func (s *StubHTTPServer) ListenAndServe() error {
	s.ListenCalls++
	return s.ListenErr
}

func (s *StubHTTPServer) Shutdown(ctx context.Context) error {
	_ = ctx
	s.ShutdownCalls++
	return s.ShutdownErr
}

func (s *StubHTTPServer) Addr() string {
	return s.AddrVal
}

func (s *StubHTTPServer) Handler() http.Handler {
	return s.HandlerVal
}

// BlockingHTTPServer allows simulating a shutdown that waits on an unblock channel.
type BlockingHTTPServer struct {
	AddrVal       string
	HandlerVal    http.Handler
	ShutdownCalls int
	Unblock       chan struct{}
}

func (b *BlockingHTTPServer) ListenAndServe() error {
	return nil
}

func (b *BlockingHTTPServer) Shutdown(ctx context.Context) error {
	b.ShutdownCalls++
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.Unblock:
		return nil
	}
}

func (b *BlockingHTTPServer) Addr() string {
	return b.AddrVal
}

func (b *BlockingHTTPServer) Handler() http.Handler {
	return b.HandlerVal
}

// ErrHTTPServer returns an error on ListenAndServe; Shutdown increments a counter.
type ErrHTTPServer struct {
	ShutdownCalls int
}

func (e *ErrHTTPServer) ListenAndServe() error {
	return errors.New("listen failure")
}

func (e *ErrHTTPServer) Shutdown(ctx context.Context) error {
	_ = ctx
	e.ShutdownCalls++
	return nil
}

func (e *ErrHTTPServer) Addr() string {
	return ":0"
}

func (e *ErrHTTPServer) Handler() http.Handler {
	return http.NewServeMux()
}

// CloseableHTTPServer returns ErrServerClosed from ListenAndServe.
type CloseableHTTPServer struct {
	ShutdownCalls int
}

func (c *CloseableHTTPServer) ListenAndServe() error {
	return http.ErrServerClosed
}

func (c *CloseableHTTPServer) Shutdown(ctx context.Context) error {
	_ = ctx
	c.ShutdownCalls++
	return nil
}

func (c *CloseableHTTPServer) Addr() string {
	return ":0"
}

func (c *CloseableHTTPServer) Handler() http.Handler {
	return http.NewServeMux()
}
