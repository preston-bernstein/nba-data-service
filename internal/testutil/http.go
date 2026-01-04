package testutil

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
	if err := statusError(rr, want); err != nil {
		t.Fatalf("%v", err)
	}
}

// DecodeJSON decodes the recorder body into dest, failing the test on error.
func DecodeJSON(t *testing.T, rr *httptest.ResponseRecorder, dest any) {
	t.Helper()
	if err := decodeJSONBody(rr, dest); err != nil {
		t.Fatalf("%v", err)
	}
}

func statusError(rr *httptest.ResponseRecorder, want int) error {
	if rr.Code == want {
		return nil
	}
	body := snippet(rr.Body.String())
	return errors.New(
		formatErr("expected status %d, got %d body=%q", want, rr.Code, body),
	)
}

func decodeJSONBody(rr *httptest.ResponseRecorder, dest any) error {
	if err := json.NewDecoder(rr.Body).Decode(dest); err != nil {
		body := snippet(rr.Body.String())
		return errors.New(formatErr("failed to decode response: %v body=%q", err, body))
	}
	return nil
}

func snippet(body string) string {
	body = strings.TrimSpace(body)
	if len(body) > 512 {
		return body[:512] + "..."
	}
	return body
}

func formatErr(msg string, args ...any) string {
	return fmt.Sprintf(msg, args...)
}
