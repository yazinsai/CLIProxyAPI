#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
archive_output="${1:-cli-proxy-api}"
archive_base="$(cat tools/archive-upstream-base)"
if [[ ! "$archive_base" =~ ^[0-9a-f]{40}$ ]]; then
  echo 'Invalid archive upstream base' >&2
  exit 1
fi
archive_version="$(git describe --tags --always --dirty)+archive"
archive_commit="$(git rev-parse HEAD)"
archive_build_date="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
go build -ldflags "-X main.Version=$archive_version -X main.Commit=$archive_commit -X main.BuildDate=$archive_build_date -X github.com/router-for-me/CLIProxyAPI/v8/internal/logging.ArchiveUpstreamBase=$archive_base" -o "$archive_output" ./cmd/server
