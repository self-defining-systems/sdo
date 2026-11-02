#!/usr/bin/env bash
# All modules use Go's content-addressed compiler/module caches. No per-module
# cache or serial preparation job; package parallelism uses the runner's CPUs.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
shard="${2:-all}"
args=(-tags=integration -json -timeout=5m)
if [[ "${1:-pr}" == nightly ]]; then
    # The slowest packages run alone under -race and need far more than the
    # default per-package budget; each has its own nightly job (shard).
    args=(-tags=integration -json -timeout=40m -race -skip '^TestChaos|^TestExhaustive|^TestExplorer')
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
# Nightly shards: runtime, builder and memory (validator plus broker) hold the
# slow packages; "rest" is every other package, selected by exclusion so a new
# package lands in a shard without editing this list.
heavy='^sdo\.dev/operator/(runtime|builder|memory|broker)(/|$)'
case "$shard" in
    all) ;;
    runtime) patterns=(./sdo/operator/runtime/...) ;;
    builder) patterns=(./sdo/operator/builder/...) ;;
    memory) patterns=(./sdo/operator/memory/... ./sdo/operator/broker/...) ;;
    rest) mapfile -t patterns < <(go list -tags=integration "${patterns[@]}" | grep -Ev "$heavy") ;;
    *) echo "unknown Go test shard: $shard" >&2; exit 2 ;;
esac
# Nested builder tests must select their own isolated modules.
go test -exec 'env GOWORK=off' "${args[@]}" "${patterns[@]}"
if [[ "${1:-pr}" == nightly && ( "$shard" == all || "$shard" == runtime ) ]]; then
    go test -exec 'env GOWORK=off' -tags=integration -json -race -run '^TestChaos|^TestExhaustive|^TestExplorer' -timeout=5m ./sdo/operator/runtime/...
fi
