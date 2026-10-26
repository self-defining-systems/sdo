#!/usr/bin/env bash
# Prepare the exact normal, portable and race graphs outside operation deadlines.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export GOMAXPROCS="${GOMAXPROCS:-2}"
# Generated workspaces live at fresh paths. Trim paths in both preparation and
# consumers so identical seed packages share Go's content-addressed archives.
export GOFLAGS="${GOFLAGS:--p=2 -trimpath}"
case " $GOFLAGS " in *" -p="*) ;; *) GOFLAGS="$GOFLAGS -p=2" ;; esac
case " $GOFLAGS " in *" -trimpath "*) ;; *) GOFLAGS="$GOFLAGS -trimpath" ;; esac
mode="${1:-normal}"
case "$mode" in
  normal) build_flags=() ;;
  race) build_flags=(-race) ;;
  *) echo "unknown Go preparation mode: $mode" >&2; exit 1 ;;
esac
test_build="$(mktemp -d)"
trap 'rm -rf "$test_build"' EXIT
for module in sdo/operator sdo/operator/runtime; do
  (cd "$root/$module" && go build "${build_flags[@]}" ./...)
  destination="$test_build/$module"
  mkdir -p "$destination"
  # -c never starts TestMain or a test. Normal Python contracts need test-only
  # archives too, not just the race shards. Go still links/discovers per shard.
  (cd "$root/$module" && go test "${build_flags[@]}" -c -o "$destination/" ./...)
done
if [[ "$mode" == normal ]]; then
  # Generated traffic probers explicitly disable CGO; those archives have
  # different build IDs from the ordinary hosted graph.
  (cd "$root/sdo/operator/runtime" && CGO_ENABLED=0 go build ./...)
  # This source-bound executable is verified again by every Python consumer.
  python3 "$root/scripts/operator_test_binary.py"
  (cd "$root/sdo/operator" && go build -o "$root/.fastloop/test-bin/setup" "$root/tests/unit/benchmarks/sregym/fastloop/fixtures/setup.go")
  # Populate the compiler cache for every seed/overlay once. The race shard
  # repeats the complete acceptance gate; equal generated source now reuses
  # these archives despite a different isolated workspace directory.
  (cd "$root/sdo/operator" && go test -v -count=1 -timeout=4m -run '^TestAssuranceSeedsPassHealthJudgeCheck$' ./memory/validate)
fi
