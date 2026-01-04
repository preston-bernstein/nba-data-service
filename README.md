# NBA Data Service (Go)

[![Build](https://github.com/preston-bernstein/nba-data-service/actions/workflows/ci.yml/badge.svg)](https://github.com/preston-bernstein/nba-data-service/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-95%25-brightgreen)](coverage.out)
[![Go Version](https://img.shields.io/badge/go-1.21+-blue)](https://go.dev/doc/devel/release)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/preston-bernstein/nba-data-service)](https://goreportcard.com/report/github.com/preston-bernstein/nba-data-service)

### What it does
- Serves NBA games, teams, and players from an in-memory cache backed by filesystem snapshots.
- Polls an upstream provider (fixture or balldontlie) to warm the cache and writes snapshots to stay within API quotas.

### Endpoints
- `GET /health` — liveness.
- `GET /ready` — readiness (poller status).
- `GET /games/today` — cached games; falls back to snapshot; optional `tz`.
- `GET /games?date=YYYY-MM-DD` — snapshot for a specific date.
- `GET /games/{id}` — game by ID.
- `GET /teams` — teams in cache; optional `activeOnly=true`.
- `GET /teams/{id}` — team by ID.
- `GET /players` — players in cache; optional `activeOnly=true`.
- `GET /players/{id}` — player by ID.
- `POST /admin/snapshots/refresh?date=YYYY-MM-DD&tz=TZ` — write a snapshot (requires `ADMIN_TOKEN` header bearer token).

### Run
```sh
make run
# or
CGO_ENABLED=0 GOCACHE=$(pwd)/.cache/go-build go run ./cmd/server
```

### Test
```sh
make test
make coverage
```

### Quick curl (fixture defaults)
```sh
curl http://localhost:4000/health
curl http://localhost:4000/games/today
curl http://localhost:4000/games/fixture-1
```

### Config (env)
- `PORT` (default `4000`)
- `PROVIDER` (`fixture`|`balldontlie`, default `fixture`)
- `POLL_INTERVAL` (default `30s`)
- `BALDONTLIE_BASE_URL`, `BALDONTLIE_API_KEY` (optional), `BALDONTLIE_TIMEZONE` (default `America/New_York`), `BALDONTLIE_MAX_PAGES` (default `5`), `BALDONTLIE_TIMEOUT` (default `10s`)
- `LOG_LEVEL` (`info` default), `LOG_FORMAT` (`json` or `text`)
- Metrics/OTLP: `METRICS_ENABLED`, `METRICS_PORT`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_INSECURE`
- Snapshots: `SNAPSHOT_SYNC_ENABLED`, `SNAPSHOT_SYNC_DAYS`, `SNAPSHOT_FUTURE_DAYS`, `SNAPSHOT_SYNC_INTERVAL`, `SNAPSHOT_DAILY_HOUR`, `SNAPSHOT_TEAMS_REFRESH_DAYS`, `SNAPSHOT_PLAYERS_REFRESH_HOURS`, `SNAPSHOT_FOLDER`
- Admin: `ADMIN_TOKEN` for snapshot refresh

### Postman
- Collection: `postman/nba-data-service.postman_collection.json`
- Vars: `baseUrl` (default `http://localhost:4000`), `date`, `id`, `tz`, `adminToken`

### Snapshots
- Path: `data/snapshots/{games|teams|players}/YYYY-MM-DD.json` plus `manifest.json`.
- Syncer: backfills past/future window; daily refresh at UTC hour (configurable); teams weekly by default, players daily; skips writes when unchanged.
- Handler: caches first; falls back to snapshot when cache empty (games). Teams/players read the in-memory cache refreshed by the syncer.

### Data freshness
- Games: live poller (interval via `POLL_INTERVAL`) plus snapshot sync.
- Teams: snapshot sync (default weekly; configurable).
- Players: snapshot sync (default daily; configurable).
- `?activeOnly=true` on `/teams` or `/players` returns the latest roster view.

### Structure
- `cmd/server` — entrypoint.
- `internal/http` — router, handlers, middleware.
- `internal/app` — games/teams/players services over the shared store.
- `internal/providers` — fixture, balldontlie, retry/limit wrappers.
- `internal/snapshots` — fs store, writer, syncer.
- `internal/store` — memory cache.
- `internal/config`, `logging`, `metrics`, `poller`, `server`.

### Notes
- Module: `nba-data-service`.
- Use `LOG_FORMAT=text` and `LOG_LEVEL=debug` for local readability.
- Fixture mode makes no network calls; balldontlie respects quota via rate-limit wrapper.
