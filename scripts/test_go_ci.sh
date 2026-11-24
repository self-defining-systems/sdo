#!/usr/bin/env bash
# All modules use Go's content-addressed compiler/module caches. No per-module
# cache or serial preparation job; package parallelism uses the runner's CPUs.
# Usage: test_go_ci.sh [pr|nightly] [validate|cli|rest]
# The optional shard selects packages by directory using .github/ci_selection.json.
# Without a shard, one invocation schedules every package.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$root/scripts/isolate_git_config.sh"
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
    sdo/operator/runtime sdo/operator sdo/operator/integration
    benchmarks/sregym/protocol/native_receipt_go benchmarks/sregym/protocol/submission_go
    benchmarks/sregym/fastloop/replay_go benchmarks/lanes)
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
# A pull request run names the packages its changes can affect (scripts/ci_select.py);
# without a plan, or when the plan is the full suite, every package runs.
if [[ -n "${SDO_CI_PLAN:-}" && "$(python3 scripts/ci_select.py mode --plan "$SDO_CI_PLAN")" == selected ]]; then
    # Capture first so a selector crash aborts (set -e) instead of silently dropping the selection.
    selected_out="$(python3 scripts/ci_select.py go-patterns --plan "$SDO_CI_PLAN" --shard "${2:?a selected run needs a shard}")"
    mapfile -t patterns <<< "$selected_out"
    # Whole-tree guards (//ci:whole-tree) scan sources no changed-file mapping can attribute, so every
    # pull request runs them, with -run, even in packages the selection skipped.
    guard_out="$(python3 scripts/ci_select.py go-guards --plan "$SDO_CI_PLAN" --shard "$2")"
    mapfile -t guard_lines <<< "$guard_out"
    guard_run="${guard_lines[0]:-}"
    guard_patterns=()
    if [[ ${#guard_lines[@]} -gt 1 ]]; then
        guard_patterns=("${guard_lines[@]:1}")
    fi
    if [[ ${#patterns[@]} -gt 0 && -z "${patterns[0]}" ]]; then
        patterns=()
    fi
    if [[ ${#patterns[@]} -eq 0 && -z "$guard_run" ]]; then
        echo "no Go packages selected for the ${2} shard"
        exit 0
    fi
    echo "==> selected Go packages: ${patterns[*]:-none}; whole-tree guards: ${guard_patterns[*]:-none}"
    set -- "${1:-pr}" selected
fi
case "${2:-}" in
    "" | selected) ;;
    rest | cli | validate)
        # The package list is fixed here; go test then schedules it as one run.
        partition="$(go list -tags=integration -f '{{.ImportPath}} {{.Dir}}' "${patterns[@]}" |
            python3 scripts/ci_select.py partition-go --root "$root" --shard "$2")"
        mapfile -t patterns <<< "$partition"
        [[ ${#patterns[@]} -gt 0 && -n "${patterns[0]}" ]] || { echo "no packages in the $2 shard" >&2; exit 1; }
        ;;
    *) echo "unknown Go test shard: $2" >&2; exit 2 ;;
esac
if [[ "${1:-pr}" != nightly ]]; then
    # Quarantined flaky tests (.github/ci_quarantine.json) run in the nightly tier only.
    skip="$(python3 "$root/scripts/ci_quarantine.py" go-skip)"
    if [[ -n "$skip" ]]; then
        args+=(-skip "$skip")
    fi
fi
# Nested builder tests must select their own isolated modules.
# SDO_GO_TEST_LOG keeps the JSON stream for scripts/check_slow_tests.py.
run_go_test() {
    if [[ -n "${SDO_GO_TEST_LOG:-}" ]]; then
        go test -exec 'env GOWORK=off' "$@" | tee -a "$SDO_GO_TEST_LOG"
    else
        go test -exec 'env GOWORK=off' "$@"
    fi
}
if [[ -n "${SDO_GO_TEST_LOG:-}" ]]; then
    : > "$SDO_GO_TEST_LOG"
fi
status=0
if [[ ${#patterns[@]} -gt 0 ]]; then
    run_go_test "${args[@]}" "${patterns[@]}" || status=$?
fi
if [[ -n "${guard_run:-}" ]]; then
    # The guards run in packages the selection skipped.
    run_go_test "${args[@]}" -run "$guard_run" "${guard_patterns[@]}" || status=$?
fi
exit "$status"
