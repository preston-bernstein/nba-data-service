# NBA Data Service (Go)

This service polls upstream NBA providers, normalizes games into the shared data model, caches them in-memory (snapshots planned), and exposes HTTP endpoints consumed by the Node BFF. It ships with a fixture provider for local development and a balldontlie client for real upstream data.

## Features
- Canonical game/domain models aligned with the portfolio’s shared types.
- HTTP API: `/health`, `/games/today`, `/games/{id}`.
- In-memory store with a periodic poller that warms data on startup.
- Configurable provider selection (`fixture` default; balldontlie client available).
- Graceful shutdown and test coverage across handlers, poller, store, server wiring, and config.

## Getting Started
Prerequisites:
- Go 1.21+ available on your `PATH` (Homebrew install: `brew install go@1.21`).

Install deps (module is self-contained):
```sh
go mod tidy
```

## Configuration
Environment variables (optional; defaults shown). See `.env.example`:
- `PORT` (default `4000`)
- `POLL_INTERVAL` (default `30s`)
- `PROVIDER` (default `fixture`; `balldontlie` available)
- `BALDONTLIE_BASE_URL` (default `https://api.balldontlie.io/v1`)
- `BALDONTLIE_API_KEY` (optional; use if your balldontlie instance requires auth)
- `BALDONTLIE_TIMEZONE` (default `America/New_York`; controls which “today” date is requested from balldontlie)
- `BALDONTLIE_MAX_PAGES` (default `5`; cap on paginated fetches)
- `BALDONTLIE_TIMEOUT` (default `10s`; HTTP client timeout for balldontlie)
- `LOG_LEVEL` (optional; `debug`/`info`/`warn`/`error`, default `info`)
- `LOG_FORMAT` (optional; `json` default, or `text`)
  - Use `LOG_FORMAT=text` and `LOG_LEVEL=debug` for local development to get readable output with source hints; keep `json` for prod.
- Production config (recommended):
  - `LOG_FORMAT=json`, `LOG_LEVEL=info`
  - `METRICS_ENABLED=true`, `METRICS_PORT=9090`
  - `OTEL_EXPORTER_OTLP_ENDPOINT` set only when you have a collector; keep `OTEL_EXPORTER_OTLP_INSECURE=false` in prod
  - `BALDONTLIE_TIMEOUT=10s` (or lower if your platform is resource constrained)
- Tier presets (examples; adjust to your quota):
  - Free: `PROVIDER=balldontlie`, `POLL_INTERVAL=60s`, `BALDONTLIE_MAX_PAGES=1`, `BALDONTLIE_TIMEOUT=10s`
  - Higher tier: `PROVIDER=balldontlie`, `POLL_INTERVAL=30s`, `BALDONTLIE_MAX_PAGES=5` (or higher if allowed), `BALDONTLIE_TIMEOUT=10s`

## Run
Using Make:
```sh
make run
```
Equivalent raw command:
```sh
CGO_ENABLED=0 GOCACHE=$(pwd)/.cache/go-build go run ./cmd/server
```

VS Code: Command Palette → Run Task → `Go: Run (make run)` (tasks are preconfigured to set PATH/GOCACHE/CGO flags).

Quick curl checks (with fixture provider):
```sh
curl http://localhost:4000/health
curl http://localhost:4000/games/today
curl http://localhost:4000/games/fixture-1
```

## Endpoints
- `GET /health` → `{"status":"ok"}`
- `GET /games/today` → `{ "date": "YYYY-MM-DD", "games": [...] }`
- `GET /games?date=YYYY-MM-DD&tz=America/New_York` → games for a specific date (defaults to “today” if omitted; optional `tz` influences “today” when `date` is missing; invalid `tz` falls back to server default)
- `GET /games/{id}` → single game or 404

When using the fixture provider, `games/today` returns two deterministic sample games.

OpenAPI spec: see `api/openapi.yaml` for the service contract (mirrors the documented endpoints).

## Testing
```sh
make test
# or
CGO_ENABLED=0 GOCACHE=$(pwd)/.cache/go-build go test ./...
```

VS Code: Command Palette → Run Task → `Go: Test (make test)`.

Coverage:
```sh
make coverage        # function-level summary
go tool cover -html=coverage.out  # open line-level view
```

Benchmarks (local, darwin/arm64, Go 1.21):
```sh
CGO_ENABLED=0 GOCACHE=$(pwd)/.cache/go-build go test -bench=. -benchmem ./internal/http ./internal/poller
```
- `BenchmarkGamesToday`: ~1.9µs/op, 6.2KB allocs, 22 allocs/op
- `BenchmarkGameByID`: ~1.7µs/op, 6.0KB allocs, 18 allocs/op
- `BenchmarkPollerFetchOnce`: ~137ns/op, 448B, 3 allocs/op

## Build
```sh
make build
# or
CGO_ENABLED=0 GOCACHE=$(pwd)/.cache/go-build go build ./cmd/server
```

VS Code: Command Palette → Run Task → `Go: Build (make build)`.

### Direnv (optional, recommended for dev)
If you use `direnv`, add the hook to your shell and allow the repo to auto-load `.env`:
```sh
echo 'eval "$(direnv hook zsh)"' >> ~/.zshrc
source ~/.zshrc
direnv allow
```
An `.envrc` is included that runs `dotenv .env`. Keep secrets out of git; use `.env.example` as a template.

## Manual API Testing
- Postman collection: `postman/nba-data-service.postman_collection.json` (baseUrl defaults to `http://localhost:4000`).
- Start the server (fixture provider pre-populates data) and hit the endpoints above.

## Project Structure
- `cmd/server/` – entrypoint wiring config, HTTP server, poller.
- `internal/domain/` – models and domain service.
- `internal/store/` – thread-safe in-memory cache.
- `internal/providers/` – provider interface, fixture, and balldontlie stub.
- `internal/http/` – handlers and router.
- `internal/server/` – server orchestration and provider selection.
- `internal/config/` – env-driven configuration.
- `internal/logging/`, `internal/metrics/`, `internal/poller/` – observability, metrics placeholder, polling loop.
- `postman/` – Postman collection.
- `.vscode/` – tasks for build/test/run.

## Portfolio Notes
This repo is part of a broader portfolio. The service respects the shared data model and can be swapped from the fixture provider to a real upstream client without changing the public contract. Tests cover core behavior and edge cases across the stack. The Postman collection and VS Code tasks are included to streamline demonstration and review. No external network calls are required for the fixture path.

For the balldontlie provider, “today” is derived from a configurable server timezone (default `America/New_York`). If you later want per-user local “today,” the API would need to accept a date/timezone parameter and the UI would pass it through.

## Status
- Done: baseline server wiring, fixture provider, poller with warm-start, in-memory store, HTTP endpoints (`/health`, `/games/today`, `/games/{id}`), provider selection, balldontlie client + mapper, VS Code tasks, Postman collection, tests across handlers/poller/store/server/config.
- In progress/planned: metrics and richer logging, retry/backoff for provider errors, CI pipeline, containerization.

## Roadmap
- Implement a real balldontlie provider client and mapper to domain models.
- Add metrics (Prometheus/OpenTelemetry) and structured request logging.
- Harden error handling with retries/backoff for providers.
- Add CI checks (build/test) and optional containerization for deployment.
