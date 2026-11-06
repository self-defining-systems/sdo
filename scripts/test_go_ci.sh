#!/usr/bin/env bash
# All modules use Go's content-addressed compiler/module caches. No per-module
# cache or serial preparation job; package parallelism uses the runner's CPUs.
# Usage: test_go_ci.sh [pr|nightly] [validate|rest]
# The optional shard splits the PR run across two hosted jobs. "validate" is the
# memory validator, whose compiler-backed tests dominate wall time; "rest" is every
# other package. Without a shard, one invocation schedules every package.
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
modules=(sdo/operator/sdk sdo/operator/core sdo/operator/contracts sdo/operator/telemetry
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
validate_pattern="./sdo/operator/memory/validate"
case "${2:-}" in
    "") ;;
    validate) patterns=("$validate_pattern") ;;
    rest)
        # The package list is fixed here; go test then schedules it as one run.
        mapfile -t patterns < <(go list -tags=integration "${patterns[@]}" |
            grep -v "^sdo.dev/operator/memory/validate$" || true)
        [[ ${#patterns[@]} -gt 0 ]] || { echo "no packages outside the validate shard" >&2; exit 1; }
        ;;
    *) echo "unknown Go test shard: $2" >&2; exit 2 ;;
esac
# Nested builder tests must select their own isolated modules.
# SDO_GO_TEST_LOG keeps the JSON stream for scripts/check_slow_tests.py.
if [[ -n "${SDO_GO_TEST_LOG:-}" ]]; then
    go test -exec 'env GOWORK=off' "${args[@]}" "${patterns[@]}" | tee "$SDO_GO_TEST_LOG"
else
    go test -exec 'env GOWORK=off' "${args[@]}" "${patterns[@]}"
fi
