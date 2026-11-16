#!/usr/bin/env bash
# Install the pinned gate tools (linux amd64) into GATE_HOME and print the bin directory.
#
#   scripts/gate-tools/install.sh [tool...]     tools: python node actionlint hadolint trivy golangci-lint vale
#
# Every download is checked against a sha256 written here. Python tools use requirements.txt (--require-hashes),
# jscpd uses package-lock.json (npm ci). To bump a tool, change its version and hash together.
# Go itself comes from actions/setup-go in CI and from your own install locally.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GATE_HOME="${GATE_HOME:-$HOME/.cache/gate-tools}"
BIN="$GATE_HOME/bin"
mkdir -p "$BIN" "$GATE_HOME/dl"

ACTIONLINT_VERSION=1.7.12
ACTIONLINT_SHA256=8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8
HADOLINT_VERSION=2.15.1
HADOLINT_SHA256=c7187db94eeeeca956519a6af171adc31453941a1e777961f6e680f697c8c507
TRIVY_VERSION=0.76.2
TRIVY_SHA256=bc0fc726e017b8927a62b2d3531d7a84ee01769daf5c99ed866dced54089fdbf
GOLANGCI_VERSION=2.15.0
GOLANGCI_SHA256=9b7fe6a6bf6334b98decf8d86b11415ea7889afdabdd1959fdce23af2a9ebcea
VALE_VERSION=3.27.0
VALE_SHA256=c80d62f6bce87b81843ac5b512a26e63213927b957adba0a64fc594570da2000

fetch() { # url sha256 -> prints the downloaded path
  local file="$GATE_HOME/dl/${1##*/}"
  curl -fsSL "$1" -o "$file"
  echo "$2  $file" | sha256sum -c --quiet - || { echo "install: checksum mismatch for $1" >&2; exit 1; }
  echo "$file"
}

install_python() {
  python3 -m venv "$GATE_HOME/venv"
  "$GATE_HOME/venv/bin/pip" install --quiet --require-hashes -r "$HERE/requirements.txt"
  ln -sf "$GATE_HOME/venv/bin/lizard" "$GATE_HOME/venv/bin/zizmor" "$BIN/"
}
install_node() {
  cp "$HERE/package.json" "$HERE/package-lock.json" "$GATE_HOME/"
  (cd "$GATE_HOME" && npm ci --ignore-scripts --quiet)
  ln -sf "$GATE_HOME/node_modules/.bin/jscpd" "$BIN/jscpd"
}
install_actionlint() {
  local f; f="$(fetch "https://github.com/rhysd/actionlint/releases/download/v${ACTIONLINT_VERSION}/actionlint_${ACTIONLINT_VERSION}_linux_amd64.tar.gz" "$ACTIONLINT_SHA256")"
  tar -xzf "$f" -C "$BIN" actionlint
}
install_hadolint() {
  local f; f="$(fetch "https://github.com/hadolint/hadolint/releases/download/v${HADOLINT_VERSION}/hadolint-linux-x86_64" "$HADOLINT_SHA256")"
  install -m 0755 "$f" "$BIN/hadolint"
}
install_trivy() {
  local f; f="$(fetch "https://github.com/aquasecurity/trivy/releases/download/v${TRIVY_VERSION}/trivy_${TRIVY_VERSION}_Linux-64bit.tar.gz" "$TRIVY_SHA256")"
  tar -xzf "$f" -C "$BIN" trivy
}
install_golangci-lint() {
  local f; f="$(fetch "https://github.com/golangci/golangci-lint/releases/download/v${GOLANGCI_VERSION}/golangci-lint-${GOLANGCI_VERSION}-linux-amd64.tar.gz" "$GOLANGCI_SHA256")"
  tar -xzf "$f" -C "$BIN" --strip-components=1 "golangci-lint-${GOLANGCI_VERSION}-linux-amd64/golangci-lint"
}
install_vale() {
  local f; f="$(fetch "https://github.com/errata-ai/vale/releases/download/v${VALE_VERSION}/vale_${VALE_VERSION}_Linux_64-bit.tar.gz" "$VALE_SHA256")"
  tar -xzf "$f" -C "$BIN" vale
}

tools=("$@")
[ "${#tools[@]}" -gt 0 ] || tools=(python node actionlint hadolint trivy golangci-lint vale)
for t in "${tools[@]}"; do "install_$t"; done
echo "$BIN"
