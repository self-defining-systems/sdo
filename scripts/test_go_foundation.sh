#!/usr/bin/env bash
# Keep the small module contracts on shared runners without raising Go floors.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
case "${1:?select stable or contracts}" in
  stable)
    modules=(sdo/operator/sdk sdo/operator/core)
    export GOTOOLCHAIN=go1.24.5
    ;;
  current)
    modules=(sdo/operator/sdk sdo/operator/core)
    export GOTOOLCHAIN=go1.26.8
    ;;
  contracts)
    modules=(sdo/operator/contracts benchmarks/sregym/protocol/native_receipt_go benchmarks/sregym/protocol/submission_go)
    export GOTOOLCHAIN=go1.26.8
    ;;
  *) echo 'select stable or contracts' >&2; exit 2 ;;
esac
for module in "${modules[@]}"; do
  echo "==> race contracts: $module ($GOTOOLCHAIN)"
  (cd "$root/$module" && go test -race -parallel=2 -count=1 -timeout=3m ./...)
done
