#!/usr/bin/env bash
# Prepare shared archives without starting tests or TestMain.
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

# Eight runtime runners otherwise compile the same large external-test package
# during inventory discovery. Publish that exact race archive once at the same
# source path/toolchain; -c never starts TestMain or any test. Its disposable
# executable is not an artifact: shards retain their own discovery and linking.
if [[ "$mode" == race ]]; then
  runtime_test_build="$(mktemp -d)"
  trap 'rm -rf "$runtime_test_build"' EXIT
  (cd "$root/sdo/operator/runtime" && go test -race -c -o "$runtime_test_build/runtime.test" .)
  # The remaining-operator job previously spent ~67s compiling test-only
  # dependencies before its first test. Publish these archives once too.
  mkdir "$runtime_test_build/operator"
  (cd "$root/sdo/operator" && go test -race -c -o "$runtime_test_build/operator/" ./...)
fi
