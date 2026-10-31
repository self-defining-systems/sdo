#!/usr/bin/env bash
# AST scan (no package loading), including tests and inactive build variants.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
base="${SDO_DETERMINISM_BASE:-origin/main}"
base="$(git merge-base "$base" HEAD)"
committed="$(mktemp)"
trap 'rm -f "$committed"' EXIT
if git cat-file -e "$base:scripts/go_determinism_allowlist.txt" 2>/dev/null; then
    git show "$base:scripts/go_determinism_allowlist.txt" > "$committed"
else
    # Initial rollout only: before this ratchet existed there is no baseline.
    # Subsequent PRs compare to main, so regeneration cannot bless additions.
    cp scripts/go_determinism_allowlist.txt "$committed"
fi
cd sdo/operator
go test ./internal/purity/determinism -count=1
go run ./internal/purity/determinism/cmd -root "$root" -baseline "$committed" "$@"
