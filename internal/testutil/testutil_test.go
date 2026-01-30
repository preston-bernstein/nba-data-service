package testutil

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	domaingames "github.com/preston-bernstein/nba-data-service/internal/domain/games"
	"github.com/preston-bernstein/nba-data-service/internal/providers"
)

func TestClockHelpers(t *testing.T) {
	now := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	if got := NowAt(now)(); !got.Equal(now) {
		t.Fatalf("expected fixed time, got %v", got)
	}
	if MustParseRFC3339(now.Format(time.RFC3339)) != now {
		t.Fatalf("expected parse round trip")
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on invalid RFC3339")
		}
	}()
	MustParseRFC3339("not-a-time")
}

func TestFixturesHelper(t *testing.T) {
	g := SampleGame("id-1")
	if g.ID != "id-1" || g.HomeTeam.ID == "" || g.AwayTeam.ID == "" {
		t.Fatalf("unexpected game fixture %+v", g)
	}
	resp := SampleTodayResponse("2024-01-01", "id-1")
	if resp.Date != "2024-01-01" || len(resp.Games) != 1 {
		t.Fatalf("unexpected today response %+v", resp)
	}
	team := SampleTeam("t1")
	if team.ID != "t1" || team.FullName == "" {
		t.Fatalf("unexpected team fixture %+v", team)
	}
}

func TestServeHelpers(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})

	rr := Serve(handler, http.MethodPost, "/test", strings.NewReader("{}"))
	AssertStatus(t, rr, http.StatusCreated)
	var body map[string]bool
	DecodeJSON(t, rr, &body)
	if !body["ok"] {
		t.Fatalf("expected ok=true")
	}

	req := httptest.NewRequest(http.MethodGet, "/req", nil)
	rr2 := ServeRequest(handler, req)
	AssertStatus(t, rr2, http.StatusCreated)
}

func TestSnapshotHelpers(t *testing.T) {
	w := NewTempWriter(t, 5)
	date := time.Now().UTC().Format(time.DateOnly)
	WriteSnapshot(t, w, date)
	path := SnapshotPath(w, date)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("expected snapshot file, got %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("expected snapshot contents")
	}
}

func TestHTTPHelperErrorFormatting(t *testing.T) {
	rr := httptest.NewRecorder()
	rr.WriteHeader(http.StatusBadRequest)
	if _, err := rr.WriteString(strings.Repeat("x", 600)); err != nil {
		t.Fatalf("expected write success, got %v", err)
	}

	if err := statusError(rr, http.StatusOK); err == nil {
		t.Fatalf("expected status error")
	} else if !strings.Contains(err.Error(), "body=") {
		t.Fatalf("expected body snippet in error, got %v", err)
	}

	rr = httptest.NewRecorder()
	rr.WriteHeader(http.StatusOK)
	if err := statusError(rr, http.StatusOK); err != nil {
		t.Fatalf("expected nil error when status matches, got %v", err)
	}

	rr = httptest.NewRecorder()
	if _, err := rr.WriteString(`{"ok":true}`); err != nil {
		t.Fatalf("expected write success, got %v", err)
	}
	if err := decodeJSONBody(rr, &map[string]any{}); err != nil {
		t.Fatalf("expected decode success, got %v", err)
	}
	rr = httptest.NewRecorder()
	if _, err := rr.WriteString("not-json"); err != nil {
		t.Fatalf("expected write success, got %v", err)
	}
	var dest map[string]any
	if err := decodeJSONBody(rr, &dest); err == nil {
		t.Fatalf("expected decode error")
	}
}

func TestWriteSnapshotPayloadHandlesNilWriter(t *testing.T) {
	if err := writeSnapshotPayload(nil, "2024-01-01"); err == nil {
		t.Fatalf("expected error for nil writer")
	}
}

func TestServerStubs(t *testing.T) {
	p := &StubPoller{Err: errors.New("stop")}
	p.Start(context.Background())
	if err := p.Stop(context.Background()); !errors.Is(err, p.Err) {
		t.Fatalf("expected stop error")
	}
	if p.StartCalls != 1 || p.StopCalls != 1 {
		t.Fatalf("unexpected call counts %+v", p)
	}

	sh := &StubHTTPServer{ListenErr: errors.New("boom"), ShutdownErr: errors.New("down")}
	sh.HandlerVal = http.NewServeMux()
	_ = sh.ListenAndServe()
	_ = sh.Shutdown(context.Background())
	_ = sh.Handler()
	_ = sh.Addr()
	if sh.ListenCalls != 1 || sh.ShutdownCalls != 1 {
		t.Fatalf("expected listen/shutdown calls, got %+v", sh)
	}

	b := &BlockingHTTPServer{Unblock: make(chan struct{}), HandlerVal: http.NewServeMux()}
	if err := b.ListenAndServe(); err != nil {
		t.Fatalf("expected nil listen error for blocking server")
	}
	done := make(chan error, 1)
	go func() { done <- b.Shutdown(context.Background()) }()
	close(b.Unblock)
	_ = b.Handler()
	if b.Addr() != b.AddrVal {
		t.Fatalf("expected blocking server addr passthrough")
	}
	if err := <-done; err != nil {
		t.Fatalf("expected nil shutdown err, got %v", err)
	}
	if b.ShutdownCalls != 1 {
		t.Fatalf("expected shutdown called once")
	}

	e := &ErrHTTPServer{}
	_ = e.ListenAndServe()
	_ = e.Shutdown(context.Background())
	_ = e.Handler()
	if e.Addr() == "" {
		t.Fatalf("expected addr from ErrHTTPServer")
	}
	if e.ShutdownCalls != 1 {
		t.Fatalf("expected shutdown call for ErrHTTPServer")
	}

	c := &CloseableHTTPServer{}
	_ = c.ListenAndServe()
	_ = c.Shutdown(context.Background())
	_ = c.Handler()
	if c.Addr() == "" {
		t.Fatalf("expected addr from CloseableHTTPServer")
	}
	if c.ShutdownCalls != 1 {
		t.Fatalf("expected shutdown call for CloseableHTTPServer")
	}

	// verify Status passthrough
	if p.Status() != p.StatusVal {
		t.Fatalf("expected status passthrough")
	}
}

func TestLoggerAndMetricsHelpers(t *testing.T) {
	logger, buf := NewBufferLogger()
	logger.Info("hello", "k", "v")
	if buf.Len() == 0 {
		t.Fatalf("expected buffered log output")
	}
	rec, shutdown := NewRecorderWithShutdown()
	if rec == nil || shutdown == nil {
		t.Fatalf("expected recorder and shutdown")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("expected nil shutdown error, got %v", err)
	}
}

func TestProviderHelpers(t *testing.T) {
	ctx := context.Background()
	g := []domaingames.Game{{ID: "g1"}}

	p := GoodProvider{Games: g}
	if got, _ := p.FetchGames(ctx, "", ""); len(got) != 1 {
		t.Fatalf("expected games from GoodProvider")
	}

	errProv := ErrProvider{Err: errors.New("boom")}
	if _, err := errProv.FetchGames(ctx, "", ""); !errors.Is(err, errProv.Err) {
		t.Fatalf("expected error passthrough")
	}

	empty := EmptyProvider{}
	if got, err := empty.FetchGames(ctx, "", ""); err != nil || len(got) != 0 {
		t.Fatalf("expected empty result, got %v err %v", got, err)
	}

	unavail := UnavailableProvider{}
	if _, err := unavail.FetchGames(ctx, "", ""); !errors.Is(err, providers.ErrProviderUnavailable) {
		t.Fatalf("expected provider unavailable")
	}

	notify := &NotifyingProvider{Games: g, Notify: make(chan struct{}, 1)}
	if _, err := notify.FetchGames(ctx, "", ""); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	select {
	case <-notify.Notify:
	default:
		t.Fatalf("expected notify channel to close or signal")
	}
}
