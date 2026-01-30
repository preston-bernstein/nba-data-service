package handlers

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/snapshots"
	"github.com/preston-bernstein/nba-data-service/internal/teststubs"
	"github.com/preston-bernstein/nba-data-service/internal/testutil"
)

func callRefresh(t *testing.T, h *AdminHandler, method, path, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rr := httptest.NewRecorder()
	h.RefreshSnapshots(rr, req)
	return rr
}

func TestAdminRefreshRequiresAuth(t *testing.T) {
	h := NewAdminHandler(nil, nil, "secret", nil)
	rr := callRefresh(t, h, http.MethodPost, "/admin/snapshots/refresh", "")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestAdminRefreshWritesSnapshot(t *testing.T) {
	writer := snapshots.NewWriter(t.TempDir(), 1)
	provider := &teststubs.StubProvider{
		Games: []domaingames.Game{{ID: "g1"}},
	}
	h := NewAdminHandler(writer, provider, "secret", nil)

	rr := callRefresh(t, h, http.MethodPost, "/admin/snapshots/refresh?date=2024-01-01", "secret")

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestAdminRefreshRejectsWrongMethod(t *testing.T) {
	h := NewAdminHandler(nil, nil, "secret", nil)
	rr := callRefresh(t, h, http.MethodGet, "/admin/snapshots/refresh", "secret")
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rr.Code)
	}
}

func TestAdminRefreshRejectsInvalidDate(t *testing.T) {
	writer := snapshots.NewWriter(t.TempDir(), 1)
	provider := &teststubs.StubProvider{
		Games: []domaingames.Game{{ID: "g1"}},
	}
	h := NewAdminHandler(writer, provider, "secret", nil)

	rr := callRefresh(t, h, http.MethodPost, "/admin/snapshots/refresh?date=bad-date", "secret")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestAdminRefreshValidatesTimezone(t *testing.T) {
	h := NewAdminHandler(snapshots.NewWriter(t.TempDir(), 1), &teststubs.StubProvider{Games: []domaingames.Game{{ID: "g1"}}}, "secret", nil)

	rr := callRefresh(t, h, http.MethodPost, "/admin/snapshots/refresh?tz=bad/tz", "secret")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad tz, got %d", rr.Code)
	}
}

func TestAdminRefreshNoGames(t *testing.T) {
	h := NewAdminHandler(snapshots.NewWriter(t.TempDir(), 1), &teststubs.StubProvider{Games: []domaingames.Game{}}, "secret", nil)

	rr := callRefresh(t, h, http.MethodPost, "/admin/snapshots/refresh?date=2024-01-01", "secret")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for no games, got %d", rr.Code)
	}
}

func TestAdminRefreshHandlesWriterError(t *testing.T) {
	// Base path is a file; writer will fail.
	tmpFile := filepath.Join(t.TempDir(), "notadir")
	if err := os.WriteFile(tmpFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("failed to create placeholder file: %v", err)
	}
	writer := snapshots.NewWriter(tmpFile, 1)
	h := NewAdminHandler(writer, &teststubs.StubProvider{Games: []domaingames.Game{{ID: "g1"}}}, "secret", nil)

	rr := callRefresh(t, h, http.MethodPost, "/admin/snapshots/refresh?date=2024-01-01", "secret")
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on write failure, got %d", rr.Code)
	}
}

func TestAdminRefreshRequiresConfiguredWriterAndProvider(t *testing.T) {
	h := NewAdminHandler(nil, nil, "secret", nil)
	rr := callRefresh(t, h, http.MethodPost, "/admin/snapshots/refresh?date=2024-01-01", "secret")
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when writer/provider missing, got %d", rr.Code)
	}
}

func TestAdminRefreshMethodNotAllowed(t *testing.T) {
	h := NewAdminHandler(nil, nil, "secret", nil)
	rr := callRefresh(t, h, http.MethodGet, "/admin/snapshots/refresh", "")
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rr.Code)
	}
}

func TestAdminRefreshProviderError(t *testing.T) {
	writer := snapshots.NewWriter(t.TempDir(), 1)
	provider := &teststubs.StubProvider{Err: errors.New("boom")}
	logger, buf := testutil.NewBufferLogger()
	h := NewAdminHandler(writer, provider, "secret", logger)

	rr := callRefresh(t, h, http.MethodPost, "/admin/snapshots/refresh?date=2024-01-01", "secret")
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 on provider error, got %d", rr.Code)
	}
	if buf.Len() == 0 {
		t.Fatalf("expected provider error to be logged")
	}
}

func TestAdminRefreshUnauthorizedLogsClientIP(t *testing.T) {
	logger, buf := testutil.NewBufferLogger()
	h := NewAdminHandler(nil, nil, "secret", logger)
	req := httptest.NewRequest(http.MethodPost, "/admin/snapshots/refresh", nil)
	req.Header.Set("X-Forwarded-For", "10.0.0.1, 10.0.0.2")
	rr := httptest.NewRecorder()

	h.RefreshSnapshots(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
	if buf.Len() == 0 {
		t.Fatalf("expected unauthorized access to be logged")
	}
}

func TestAuthorizeHandlesEmptyToken(t *testing.T) {
	h := NewAdminHandler(nil, nil, "", nil)
	if h.authorize(httptest.NewRequest(http.MethodGet, "/", nil)) {
		t.Fatalf("expected empty token to be unauthorized")
	}
}

func TestClientIPFromForwardedHeader(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Forwarded-For", "1.2.3.4, 5.6.7.8")
	if got := clientIP(req); got != "1.2.3.4" {
		t.Fatalf("expected first forwarded address, got %s", got)
	}
}

func TestClientIPFallsBackToRemoteAddr(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "9.9.9.9:1234"
	if got := clientIP(req); got != "9.9.9.9:1234" {
		t.Fatalf("expected remote addr fallback, got %s", got)
	}
}

func TestClientIPHandlesNilRequest(t *testing.T) {
	if got := clientIP(nil); got != "" {
		t.Fatalf("expected empty string for nil request, got %q", got)
	}
}

func TestAdminTokenFromEnv(t *testing.T) {
	t.Setenv("ADMIN_TOKEN", "abc")
	if got := AdminTokenFromEnv(); got != "abc" {
		t.Fatalf("expected token from env, got %s", got)
	}
}
