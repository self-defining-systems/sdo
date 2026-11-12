#!/bin/bash
# docs/config.md is generated from contracts/settings; --check fails on drift.
set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT/sdo/operator"
target="$PROJECT_ROOT/docs/config.md"
if [[ "${1:-}" == "--check" ]]; then
    temp="$(mktemp)"
    trap 'rm -f "$temp"' EXIT
    go run ./cmd/configdoc "$temp"
    if ! diff -u "$target" "$temp"; then
        echo "docs/config.md is out of date: run scripts/gen_config_docs.sh" >&2
        exit 1
    fi
else
    go run ./cmd/configdoc "$target"
fi
