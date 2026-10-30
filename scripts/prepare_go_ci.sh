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
# The test jobs reuse this wrapper to share linked binaries between tests.
mkdir -p "$root/.fastloop/test-bin"
go build -o "$root/.fastloop/test-bin/linkcache" "$root/scripts/linkcache/main.go"
mode="${1:-normal}"
case "$mode" in
  normal) build_flags=() ;;
  race) build_flags=(-race) ;;
  *) echo "unknown Go preparation mode: $mode" >&2; exit 1 ;;
esac
echo "Preparation mode=$mode CPUs=$(getconf _NPROCESSORS_ONLN) GOMAXPROCS=$GOMAXPROCS"
for module in sdo/operator sdo/operator/runtime; do
  echo "Preparing $module ($mode) at $(date -u +%FT%TZ)"
  # The inventory must use the consumer's race build tags. Discover once while
  # compiling that graph; normal Python contracts still need their own archives.
  if [[ "$mode" == race ]]; then
    scope=operator
    if [[ "$module" == sdo/operator/runtime ]]; then scope=runtime; fi
    mkdir -p "$root/.fastloop/test-bin"
    # Go generates the canonical test tables while exporting the test archive.
    # Read those tables without linking and discarding every test executable.
    go run "$root/scripts/go_test_inventory.go" "$root/$module" > "$root/.fastloop/test-bin/inventory-$scope.json"
  else
    # Normal consumers need production archives and source-bound tools; Go
    # tests execute with -race. Linking normal test binaries only to discard
    # them doubles the cold preparation work.
    (cd "$root/$module" && go build "-p=$GOMAXPROCS" ./...)
  fi
  echo "Prepared $module ($mode) at $(date -u +%FT%TZ)"
  if [[ "$mode" == race && "$module" == sdo/operator && -n "${SDO_GO_OPERATOR_READY:-}" ]]; then
    # A FIFO receipt lets the CI owner start operator quality checks while
    # this same bounded preparation command compiles the runtime graph.
    printf 'ready\n' > "$SDO_GO_OPERATOR_READY"
  fi
done
if [[ "$mode" == normal ]]; then
  # Python static analysis vets benchmark mains against the same source graph.
  bash "$root/scripts/check_go_mains.sh"
  # Generated traffic probers explicitly disable CGO; those archives have
  # different build IDs from the ordinary hosted graph.
  (cd "$root/sdo/operator/runtime" && CGO_ENABLED=0 go build ./prober)
  # This source-bound executable is verified again by every Python consumer.
  python3 "$root/scripts/operator_test_binary.py"
  (cd "$root/sdo/operator" && go build -o "$root/.fastloop/test-bin/setup" "$root/tests/unit/benchmarks/sregym/fastloop/fixtures/setup.go")
fi
if [[ "$mode" == normal ]]; then
  # The controller jobs run with GOPROXY=off. Generated fixture workspaces run
  # `go mod tidy`/`-mod=mod`, which also needs transitive modules' go.mod files
  # outside the build graph; `all` covers them (as testutil.OfflineGo does).
  # Every module the controller jobs test (operator and benchmark protocol
  # modules), so a new module is covered without editing this.
  git -C "$root" ls-files 'sdo/operator/go.mod' 'sdo/operator/**/go.mod' 'benchmarks/sregym/protocol/**/go.mod' | grep -v /testdata/ |
    while read -r manifest; do
      bash "$root/scripts/download_go_modules.sh" "$root/$(dirname "$manifest")"
    done
fi
