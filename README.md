# NBA Games Service (Go)

This service polls upstream NBA providers, normalizes games into the shared data model, caches them in-memory, and exposes HTTP endpoints consumed by the Node BFF. It currently ships with a fixture provider for local development and wiring for future providers (e.g., balldontlie).

## Features
- Canonical game/domain models aligned with the portfolio’s shared types.
- HTTP API: `/health`, `/games/today`, `/games/{id}`.
- In-memory store with a periodic poller that warms data on startup.
- Configurable provider selection (`fixture` default; balldontlie stubbed for now).
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
- `PROVIDER` (default `fixture`; `balldontlie` stub available)

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
- `GET /games/{id}` → single game or 404

When using the fixture provider, `games/today` returns two deterministic sample games.

## Testing
```sh
make test
# or
CGO_ENABLED=0 GOCACHE=$(pwd)/.cache/go-build go test ./...
```

VS Code: Command Palette → Run Task → `Go: Test (make test)`.

## Build
```sh
make build
# or
CGO_ENABLED=0 GOCACHE=$(pwd)/.cache/go-build go build ./cmd/server
```

VS Code: Command Palette → Run Task → `Go: Build (make build)`.

## Manual API Testing
- Postman collection: `postman/nba-games-service.postman_collection.json` (baseUrl defaults to `http://localhost:4000`).
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

## Status
- Done: baseline server wiring, fixture provider, poller with warm-start, in-memory store, HTTP endpoints (`/health`, `/games/today`, `/games/{id}`), provider selection, VS Code tasks, Postman collection, tests across handlers/poller/store/server/config.
- In progress/planned: real upstream provider integration (balldontlie client + mapper), metrics and richer logging, retry/backoff for provider errors, CI pipeline, containerization.

## Roadmap
- Implement a real balldontlie provider client and mapper to domain models.
- Add metrics (Prometheus/OpenTelemetry) and structured request logging.
- Harden error handling with retries/backoff for providers.
- Add CI checks (build/test) and optional containerization for deployment.
