#!/usr/bin/env bash
# Prepare shared archives without linking or starting every test binary.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export GOMAXPROCS="${GOMAXPROCS:-2}"
export GOFLAGS="${GOFLAGS:-} -p=2"
# Test binaries are linked in their owning shards. Running go test here links
# every package (and invokes TestMain) serially before any shard can start.
for module in sdo/operator sdo/operator/runtime; do
  (cd "$root/$module" && go build ./... && go build -race ./...)
done
