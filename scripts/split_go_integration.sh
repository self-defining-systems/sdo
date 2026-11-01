#!/usr/bin/env bash
# AST-based test migration; default is a report/dry run, never a write.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root/sdo/operator"
go run ./internal/purity/testsplit/cmd "$@"
