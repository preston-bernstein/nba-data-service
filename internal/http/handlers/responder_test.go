package handlers

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/preston-bernstein/nba-data-service/internal/testutil"
)

func TestWriteErrorIncludesRequestID(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	logger, _ := testutil.NewBufferLogger()

	req.Header.Set("X-Request-ID", "abc123")

	rr := testutil.ServeRequest(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, http.StatusTeapot, "boom", logger)
	}), req)

	if rr.Code != http.StatusTeapot {
		t.Fatalf("expected status 418, got %d", rr.Code)
	}
	if got := rr.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("expected content type json, got %s", got)
	}
	body := rr.Body.String()
	if !bytes.Contains([]byte(body), []byte("abc123")) {
		t.Fatalf("expected requestId in body, got %s", body)
	}
}

func TestWriteJSONLogsEncodeError(t *testing.T) {
	logger, buf := testutil.NewBufferLogger()
	rr := testutil.Serve(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, make(chan int), logger)
	}), http.MethodGet, "/encode-error", nil)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status written even on encode error, got %d", rr.Code)
	}
	if buf.Len() == 0 {
		t.Fatalf("expected logger to record encode error")
	}
}

func TestWriteErrorFallsBackToHeaderRequestID(t *testing.T) {
	logger, _ := testutil.NewBufferLogger()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("X-Request-ID", "header-id")
	writeError(rr, req, http.StatusTeapot, "boom", logger)
	if !bytes.Contains(rr.Body.Bytes(), []byte("header-id")) {
		t.Fatalf("expected header request id used when context missing")
	}
}
