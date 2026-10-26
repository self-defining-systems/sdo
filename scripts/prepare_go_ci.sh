#!/usr/bin/env bash
# Prepare shared archives without linking or starting every test binary.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export GOMAXPROCS="${GOMAXPROCS:-2}"
export GOFLAGS="${GOFLAGS:-} -p=2"
# Each hosted mode has its own runner/cache. Compiling normal and race stdlib
# serially on one cold runner consumed the whole five-minute job deadline.
mode="${1:-normal}"
case "$mode" in
  normal) build_flags=() ;;
  race) build_flags=(-race) ;;
  *) echo "unknown Go preparation mode: $mode" >&2; exit 1 ;;
esac
# Shards own test-binary linking and execution, including TestMain.
for module in sdo/operator sdo/operator/runtime; do
  (cd "$root/$module" && go build "${build_flags[@]}" ./...)
done
