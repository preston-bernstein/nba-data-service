package testutil

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Serve executes a request against the provided handler and returns the recorder.
func Serve(h http.Handler, method, path string, body io.Reader) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, body)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

// ServeRequest executes the given request against the handler.
func ServeRequest(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

// AssertStatus verifies the response status code.
func AssertStatus(t *testing.T, rr *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rr.Code != want {
		t.Fatalf("expected status %d, got %d", want, rr.Code)
	}
}

// DecodeJSON decodes the recorder body into dest, failing the test on error.
func DecodeJSON(t *testing.T, rr *httptest.ResponseRecorder, dest any) {
	t.Helper()
	if err := json.NewDecoder(rr.Body).Decode(dest); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
}
