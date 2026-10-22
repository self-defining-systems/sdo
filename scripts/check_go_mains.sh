#!/usr/bin/env bash
# Compile every Go program under benchmarks/ against the current operator API.
# Those programs sit outside the operator tests, so a runtime API change can
# break them without failing `go test`; this catches it in seconds.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Standalone modules (own go.mod, fixtures and generated seeds excluded).
while IFS= read -r mod; do
    (cd "$(dirname "$mod")" && go vet ./...)
done < <(find "$root/benchmarks" -name go.mod -not -path '*/seeds/*' -not -path '*/operational_memory/*' | sort)

# Programs with no module of their own run via `go run` inside the operator
# runtime module (for example fastloop replay), so vet them in that context.
while IFS= read -r program; do
    dir="$(dirname "$program")"
    while [[ "$dir" != "$root" && ! -f "$dir/go.mod" ]]; do dir="$(dirname "$dir")"; done
    [[ "$dir" != "$root" ]] && continue
    (cd "$root/sdo/operator/runtime" && go vet "$program")
done < <(grep -rl --include='*.go' '^package main' "$root/benchmarks" | sort)
