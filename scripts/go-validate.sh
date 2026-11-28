#!/usr/bin/env bash
# Validation gate for agent-written Go. Pack: go, rule go-one-gate-command.
#
#   go-validate.sh [ROOT]
#
# Order: go mod verify (skipped when vendor/ exists) -> gofmt -l (must list nothing) -> go vet -> golangci-lint run -> go test -race.
# Every tool is checked before any step runs. A missing tool fails the gate with exit 127. Nothing is skipped.
set -euo pipefail
# A git hook exports GIT_DIR, GIT_INDEX_FILE and the like. A test that runs git in a temp folder would inherit them
# and write into this repo instead, so drop git's repo-local variables before any tool runs.
while IFS= read -r v; do unset "$v"; done < <(git rev-parse --local-env-vars 2>/dev/null || true)

ROOT="${1:-.}"
cd "$ROOT"

fail() {
  echo "go-validate: $*" >&2
  exit "${2:-1}"
}
need() { command -v "$1" >/dev/null 2>&1 || fail "missing tool: $1 (the gate never skips a step)" 127; }

if [ ! -f go.mod ]; then
  echo "go-validate: no go.mod, nothing to check"
  exit 0
fi

need go
need gofmt
need golangci-lint

# go mod verify downloads modules. With vendor/ present the build uses the vendored copy (checked when it was vendored),
# so the gate stays offline-safe; the story-runner sandbox has no network.
if [ ! -f vendor/modules.txt ]; then
  go mod verify
fi
listed="$(gofmt -l .)"
unformatted="$(printf '%s\n' "$listed" | grep -v -e '^vendor/' -e '^$' || true)"
if [ -n "$unformatted" ]; then
  echo "$unformatted" >&2
  fail "these files are not gofmt-formatted (run gofmt -w)"
fi
go vet ./...
golangci-lint run ./...
go test -race -count=1 ./...
echo "go-validate: all checks passed"
