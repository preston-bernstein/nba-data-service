# NBA Data Service (Go)

[![Build](https://github.com/preston-bernstein/nba-data-service/actions/workflows/ci.yml/badge.svg)](https://github.com/preston-bernstein/nba-data-service/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/preston-bernstein/nba-data-service/branch/main/graph/badge.svg)](https://codecov.io/gh/preston-bernstein/nba-data-service)
[![Go Version](https://img.shields.io/badge/go-1.21+-blue)](https://go.dev/doc/devel/release)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/preston-bernstein/nba-data-service)](https://goreportcard.com/report/github.com/preston-bernstein/nba-data-service)

### What it does
- Serves NBA games from an in-memory cache backed by filesystem snapshots.
- Polls an upstream provider (fixture or balldontlie) to warm the cache and writes/refreshes data to stay within API quotas.
- Focuses on games only (no team/player catalogs).

### Endpoints
- `GET /health` — liveness.
- `GET /ready` — readiness (poller status).
- `GET /games?date=YYYY-MM-DD` — snapshot for a specific date (required).
- `GET /games/{id}` — game by ID.
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
curl http://localhost:4000/games?date=2024-01-01
curl http://localhost:4000/games/fixture-1
```

### Config (env)
- `PORT` (default `4000`)
- `PROVIDER` (`fixture`|`balldontlie`, default `fixture`)
- `POLL_INTERVAL` (default `30s`)
- `BALLDONTLIE_BASE_URL`, `BALLDONTLIE_API_KEY` (optional), `BALLDONTLIE_TIMEZONE` (default `America/New_York`), `BALLDONTLIE_MAX_PAGES` (default `5`), `BALLDONTLIE_TIMEOUT` (default `10s`)
- `LOG_LEVEL` (`info` default), `LOG_FORMAT` (`json` or `text`)
- Metrics/OTLP: `METRICS_ENABLED`, `METRICS_PORT`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_INSECURE`
- Snapshots: `SNAPSHOT_SYNC_ENABLED`, `SNAPSHOT_SYNC_DAYS`, `SNAPSHOT_FUTURE_DAYS`, `SNAPSHOT_SYNC_INTERVAL`, `SNAPSHOT_DAILY_HOUR`
- Admin: `ADMIN_TOKEN` for snapshot refresh
- Kafka: `KAFKA_BROKERS` (comma list, empty turns publishing off), `KAFKA_TOPIC` (default `game.updated`)

### Kafka
The poller publishes a `game.updated` event to Kafka when a game is new, or its `statusKind` or `score` changed since the last poll. The message key is the game id, so one game stays in order on one partition. The JSON shape is in `internal/events/testdata/game.updated.v1.json`.

- The last-seen state is kept in memory. After a restart, the first poll sends a `created` event for every game.
- If Kafka is down, the poller logs the error and finishes the cycle. It retries the unsent games on the next poll.
- Batches are gzip compressed. The franz-go default is snappy, which the Node consumer (kafkajs) cannot read.
- Set `KAFKA_BROKERS` to turn it on. Leave it empty and nothing is published.

Integration test (needs a broker; it skips unless `KAFKA_TEST_BROKERS` is set):

```sh
make test-integration   # uses localhost:29092 by default
```

### Postman
- Collection: `postman/nba-data-service.postman_collection.json`
- Vars: `baseUrl` (default `http://localhost:4000`), `date`, `id`, `tz`, `adminToken`

### Storage
- Games snapshots: `data/snapshots/games/YYYY-MM-DD.json` plus `manifest.json`.
- Handler: caches first; falls back to snapshot when cache empty (games).

### Data freshness
- Games: live poller (interval via `POLL_INTERVAL`) plus snapshot sync.
- **When snapshot sync runs** (if `SNAPSHOT_SYNC_ENABLED` is true, default):
  - **On startup**: one full backfill in a background goroutine (prune expired files, then refresh today, yesterday, and 2 days ago, then any missing past/future dates). There is a 90s delay between each date fetch, so the initial backfill takes several minutes. Check logs for `snapshot sync starting` and `snapshot sync initial backfill complete`.
  - **Daily**: full backfill again at the configured hour (default 2 AM UTC, `SNAPSHOT_DAILY_HOUR`).
- After a restart (e.g. `docker compose up`), wait ~5 minutes for the startup backfill to refresh the last 3 days, or call `POST /admin/snapshots/refresh` (with `ADMIN_TOKEN`) to refresh specific dates. If you use a persistent volume, old snapshot files are reused until overwritten by the backfill.

### Structure
- `cmd/server` — entrypoint.
- `internal/http` — router, handlers, middleware.
- `internal/providers` — fixture, balldontlie, retry/limit wrappers.
- `internal/snapshots` — fs store, writer, syncer.
- `internal/config`, `logging`, `metrics`, `poller`, `server`.

### Notes
- Module: `nba-data-service`.
- Use `LOG_FORMAT=text` and `LOG_LEVEL=debug` for local readability.
- Fixture mode makes no network calls; balldontlie respects quota via rate-limit wrapper.
