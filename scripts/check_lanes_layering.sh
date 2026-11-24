#!/usr/bin/env bash
# What the lanes packages may name (benchmarks/lanes/internal/layerguard). The test is integration-tagged, so a plain
# `go test` never compiles it; it reads source files only (one package, about a second).
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root/benchmarks/lanes" && go test -count=1 -tags integration ./internal/layerguard
