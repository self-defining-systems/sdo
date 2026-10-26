#!/usr/bin/env bash
# Prepare the shared normal/race dependency graph once, before bounded test jobs.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export GOMAXPROCS="${GOMAXPROCS:-2}"
export GOFLAGS="${GOFLAGS:-} -p=2"
(cd "$root/sdo/operator" && go build -o /dev/null ./cmd/sdo-operator && go test -race -run '^$' ./...)
(cd "$root/sdo/operator/runtime" && go build ./... && go test -run '^$' ./... && go test -race -run '^$' ./...)
