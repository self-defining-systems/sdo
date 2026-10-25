#!/usr/bin/env bash
# Module tests and runtime contracts share one runner and dependency caches.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
export GOMAXPROCS="${GOMAXPROCS:-2}"
export GOFLAGS="${GOFLAGS:--p=2}"

modules() {
  for module in sdo/operator/sdk sdo/operator/core sdo/operator/contracts benchmarks/sregym/protocol/native_receipt_go benchmarks/sregym/protocol/submission_go; do
    echo "==> race: $module"
    toolchain=go1.26.8
    if [[ "$module" = sdo/operator/sdk || "$module" = sdo/operator/core ]]; then
      toolchain=go1.24.5
    fi
    (cd "$module" && GOTOOLCHAIN="$toolchain" go test -race -parallel=2 -count=1 -timeout=8m -json ./...)
  done
}
# Start the longest module immediately, overlapping the lower-floor/module
# checks with its real broker/compiler tests. Three lanes use -p=2 (six builds).
(cd sdo/operator && go test -race -parallel=2 -count=1 -timeout=8m -json ./...) &
operator_pid=$!
modules &
module_pid=$!
python3 scripts/test_runtime_shard.py --shard contracts &
runtime_pid=$!
trap 'kill "$operator_pid" "$module_pid" "$runtime_pid" 2>/dev/null || true' EXIT
status=0
wait "$operator_pid" || status=$?
wait "$module_pid" || status=$?
wait "$runtime_pid" || status=$?
trap - EXIT
exit "$status"
