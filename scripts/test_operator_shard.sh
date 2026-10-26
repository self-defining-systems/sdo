#!/usr/bin/env bash
# Preserve the existing entry point; the runner owns coverage and timing guards.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 "$root/scripts/test_operator_shard.py" "$@"
