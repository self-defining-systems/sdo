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
    # During the rollout PR, main predates the list. Pin the FIRST committed
    # inventory, not the working copy or a later regenerated PR commit.
    initial="$(git log --diff-filter=A --format=%H "$base"..HEAD -- scripts/go_determinism_allowlist.txt | tail -1)"
    if [[ -z "$initial" ]]; then
        echo "Commit the initial inventory before checking the rollout." >&2
        exit 1
    fi
    git show "$initial:scripts/go_determinism_allowlist.txt" > "$committed"
fi
cd sdo/operator
go test ./internal/purity/determinism -count=1
go run ./internal/purity/determinism/cmd -root "$root" -baseline "$committed" "$@"
