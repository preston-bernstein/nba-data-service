# Maintainability Polish Pass — 2026-08-15

Scope: full-repo Go maintainability polish (dead code, readability, error-handling/observability discipline), per the standing polish process. Cost-no-object pass, conservative given this is live infrastructure deployed to desktop.example.internal, vmhost.example.internal, and a DigitalOcean VPS.

## Process

1. `git fetch origin` — local `main` was 2 commits behind (`637b225` incl. PR #2, snapshot sync backfill fix). Fast-forwarded cleanly. The `fix-snapshot-sync-past-days-off-by-one` branch this session started on had identical content to `origin/main` (already merged), so work proceeded on `main`.
2. Ran `golang.org/x/tools/cmd/deadcode` (whole-program, from `./cmd/server`, the only entrypoint) and `staticcheck ./...` for deterministic signal.
3. Manually verified every flagged symbol against production call sites before touching anything — deadcode only traces from `main()`, so it flags any symbol reachable solely from `_test.go` files, including deliberate test-DI seams. Each flag was checked individually rather than removed on the tool's say-so.
4. Applied safe fixes; re-ran deadcode/staticcheck/build/vet/test to confirm a clean baseline afterward.

## Applied (6 changes, all internal-only — zero domain-struct or OpenAPI-spec changes)

1. **Removed `handlers.AdminTokenFromEnv()`** (`internal/http/handlers/admin.go`) — orphaned duplicate. Production admin-token wiring actually goes through `config.envAdminToken` → `cfg.Snapshots.AdminToken` (see `internal/config/snapshots.go:31`, `internal/server/server.go:84`); this second `os.Getenv("ADMIN_TOKEN")` helper was never called outside its own unit test. Removed the function, its dedicated test (`TestAdminTokenFromEnv`), and the now-unused `"os"` import.
2. **Removed `logging.WithCommon()`** (`internal/logging/fields.go`) — zero production callers; service/version fields are attached at logger construction (`logging.NewLogger`) via a different path, not this helper. Removed the function and its dedicated test file (`fields_test.go`, which contained nothing else). Kept the `Field*` const block — several of those keys (`FieldStatusCode`, `FieldCount`, `FieldDurationMS`) are actively used elsewhere. Removed the now-unused `"log/slog"` import from `fields.go`.
3. **Removed `providers.ResolveTimezone()`** (`internal/providers/timezone.go`) — superseded; production timezone resolution goes through `timeutil.ResolveLocation` (`internal/server/server.go:51`) and the admin handler does its own inline `time.LoadLocation` validation. This function had no production callers, only its own tests. Deleted the file and its test file (`timezone_test.go`) — both contained nothing else.
4. **Fixed an inert lint-suppression comment** (`internal/poller/poller_test.go`) — `TestPollerStopWithNilContext` deliberately calls `p.Stop(nil)` to verify nil-safe behavior, and had a `//nolint:staticcheck` comment intended to suppress SA1012. That's golangci-lint's directive syntax, not staticcheck's own — the standalone `staticcheck` binary run in this pass ignored it and flagged the line. Replaced with the correct native directive: `//lint:ignore SA1012 testing explicit nil-context behavior`.

Net diff: 7 files touched (2 deleted), +1/-90 lines.

## Kept as false positives (do not touch)

`deadcode` also flagged these three — confirmed each is a deliberate, doc-commented seam, not dead code:

- `internal/server/server.go:68` `newServerWithDeps` — doc comment: "used for testing to inject custom components." Real DI seam, ~7 test call sites.
- `internal/server/server.go:239` `Server.Handler()` — doc comment: "exposes the HTTP handler (useful for tests)." Real test accessor, ~13 call sites across `internal/server`, `internal/http/handlers`, `internal/testutil`.

Both exist specifically to make other code testable; deadcode only flags them because its whole-program trace starts at `main()` and doesn't count `_test.go` as a root.

## Escalated, not applied — flagged for Preston's judgment

- **`internal/logging/helpers.go:20` `logging.Error()`** — deadcode flags this as unreachable in production too, but it's a different situation from the three removed above. `logging.Info` and `logging.Warn` (same file, same API shape) are used extensively in production; `logging.Error` is the missing third rung of that same API, not a superseded duplicate. Meanwhile, several real failure points currently log via `logging.Warn` with an `err` field even though they represent genuine operational failures, not input-validation warnings:
  - `internal/snapshots/sync.go:93` "snapshot wipe failed"
  - `internal/snapshots/sync.go:174` "snapshot sync write failed"
  - `internal/http/handlers/admin.go` "admin snapshot fetch failed" / "admin snapshot write failed"

  Wiring `logging.Error` into these (or any severity change from Warn→Error) is a **behavior change**, not a pure cleanup — on live infra, log severity can be tied to existing alerting/dashboards. Deleting `logging.Error` instead would leave an asymmetric, harder-to-extend logging API. Recommend: leave as-is for now, and treat "should some of these be Error-level" as a deliberate follow-up decision, not a silent polish-pass change.
- **`internal/snapshots/writer.go:68,226`** — `_ = os.Remove(path)` during backfill delete (`DeleteAllGamesSnapshots`) and retention pruning (`pruneOldSnapshots`) silently discards removal failures (e.g. permission errors — not just harmless "already gone" races). `Writer` currently has no logger field, so fixing this properly means threading a `*slog.Logger` through `NewWriter(basePath, retentionDays)`, which has ~40 call sites (mostly tests, a few production: `internal/server/snapshots_factory.go:21`). Given the "be conservative" instruction and the disproportionate blast radius vs. the low practical severity (best-effort cleanup during idempotent pruning), left untouched. Worth a dedicated small follow-up if desired.
- Everything touching `Game`/`Team`/`GameMeta`/`Score` struct shapes or `api/openapi.yaml`: **zero changes**, and none were found worth flagging — those files were not touched or altered in any way.

## Regression safety net

- `go build ./...` — clean
- `go vet ./...` — clean
- `go test ./... -cover` — all 19 packages pass; coverage unchanged or improved in every package this pass touched (`internal/http/handlers` 99.3%, `internal/logging` 96.4%, `internal/poller` 100%, `internal/providers` 96.7%)
- `gofmt -l .` — clean
- Re-ran `deadcode ./cmd/server` and `staticcheck ./...` after all edits: staticcheck fully clean; deadcode reports only the two confirmed-intentional test seams above.

## Not done (explicitly out of scope for this pass)

- No deploy or service restart on desktop.example.internal / vmhost.example.internal / the DigitalOcean VPS. To roll this out: pull `main` on each host, rebuild the Go binary (`go build ./cmd/server`), and restart the systemd unit (or equivalent) — no config/env changes needed since nothing behavior-visible changed in production wiring.
- No changes to `internal/domain/games/models.go`, `internal/domain/teams/models.go`, or `api/openapi.yaml` — confirmed via diff review; nothing in this pass touched the codegen-sensitive surface nba-analytics-hub depends on.
