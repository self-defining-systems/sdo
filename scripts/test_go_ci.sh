#!/usr/bin/env bash
# All modules use Go's content-addressed compiler/module caches. No per-module
# cache or serial preparation job; package parallelism uses the runner's CPUs.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
args=(-tags=integration -json -timeout=5m)
if [[ "${1:-pr}" == nightly ]]; then
    # -race needs far more than the default per-package budget.
    args=(-tags=integration -json -timeout=40m -race)
    # Seeded simulation campaigns run their full seed counts here; PRs run a fixed prefix.
    export SDO_SIM_FULL=1
else
    # -short skips the few tests that compile generated programs for 10 s or more
    # (they call testing.Short); nightly runs them. Every fuzz seed still runs here.
    args+=(-short)
fi
# A temporary workspace lets one go test invocation schedule packages across
# module boundaries, rather than waiting for each module's slowest test.
workspace="$(mktemp -d)"
trap 'rm -rf "$workspace"' EXIT
export GOWORK="$workspace/go.work"
modules=(sdo/operator/sdk sdo/operator/core sdo/operator/contracts
    sdo/operator/runtime sdo/operator
    benchmarks/sregym/protocol/native_receipt_go benchmarks/sregym/protocol/submission_go
    benchmarks/sregym/fastloop/replay_go)
patterns=()
{
    echo 'go 1.26.8'
    echo 'use ('
    for module in "${modules[@]}"; do
        echo "    $root/$module"
        patterns+=("./$module/...")
    done
    echo ')'
} > "$GOWORK"
cd "$root"
# Nested builder tests must select their own isolated modules.
go test -exec 'env GOWORK=off' "${args[@]}" "${patterns[@]}"
