#!/usr/bin/env bash
# All modules use Go's content-addressed compiler/module caches. No per-module
# cache or serial preparation job; package parallelism uses the runner's CPUs.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
args=(-json -timeout=5m)
if [[ "${1:-pr}" == nightly ]]; then
    args=(-json -timeout=15m -race)
else
    # Extended interleaving campaigns run nightly; ordinary controller tests
    # and every fuzz seed still run here.
    extended='^TestReplay|^TestChaos|^TestExhaustive|^TestExplorer'
    while IFS= read -r name; do
        [[ -z "$name" || "$name" == \#* ]] && continue
        extended+="|^${name}$"
    done < "$root/scripts/ci_nightly_go_tests.txt"
    args+=(-skip "$extended")
fi
# A temporary workspace lets one go test invocation schedule packages across
# module boundaries, rather than waiting for each module's slowest test.
workspace="$(mktemp -d)"
trap 'rm -rf "$workspace"' EXIT
export GOWORK="$workspace/go.work"
modules=(sdo/operator/sdk sdo/operator/core sdo/operator/contracts
    sdo/operator/runtime sdo/operator
    benchmarks/sregym/protocol/native_receipt_go benchmarks/sregym/protocol/submission_go)
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
