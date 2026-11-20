#!/usr/bin/env bash
# gofmt over every tracked Go file in every module (all of them, not a package
# list). Vendored third_party and testdata (deliberately malformed fixtures)
# are excluded. `--check` lists offenders and fails; default rewrites in place.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
command -v gofmt >/dev/null || { echo "gofmt not found" >&2; exit 1; }
files=$(git ls-files -z '*.go' | tr '\0' '\n' | grep -v -e '^third_party/' -e '/testdata/' || true)
[ -n "$files" ] || exit 0
if [ "${1:-}" = "--check" ]; then
    bad=$(echo "$files" | xargs gofmt -l)
    if [ -n "$bad" ]; then
        echo "not gofmt-clean (run scripts/format_code.sh):" >&2
        echo "$bad" | sed 's/^/    /' >&2
        exit 1
    fi
else
    echo "$files" | xargs gofmt -w
fi
