#!/usr/bin/env bash
# Two lanes share one runner and Go caches; no matrix recompiles the runtime.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
export GOMAXPROCS="${GOMAXPROCS:-2}"
export GOFLAGS="${GOFLAGS:--p=4}"

modules() {
  for module in sdo/operator/sdk sdo/operator/core sdo/operator/contracts sdo/operator benchmarks/sregym/protocol/native_receipt_go benchmarks/sregym/protocol/submission_go; do
    echo "==> race: $module"
    toolchain=go1.26.8
    if [[ "$module" = sdo/operator/sdk || "$module" = sdo/operator/core ]]; then
      toolchain=go1.24.5
    fi
    (cd "$module" && GOTOOLCHAIN="$toolchain" go test -race -parallel=2 -count=1 -timeout=8m -json ./...)
  done
}
modules &
module_pid=$!
trap 'kill "$module_pid" 2>/dev/null || true' EXIT
python3 scripts/test_runtime_shard.py --shard all &
runtime_pid=$!
# Always join both lanes, retaining failures from either.
status=0
wait "$module_pid" || status=$?
wait "$runtime_pid" || status=$?
trap - EXIT
exit "$status"
