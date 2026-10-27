#!/usr/bin/env bash
# External-process contracts execute after compilation, outside their timeouts.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
# Collection consumes the pinned harness tree. Initialize it before xdist can
# turn a prerequisite collection error into an unrelated worker-finished crash.
uv run python scripts/init_test_prereqs.py
export GOMAXPROCS="${GOMAXPROCS:-2}"
export GOFLAGS="${GOFLAGS:--p=2 -trimpath}"
case " $GOFLAGS " in *" -p="*) ;; *) GOFLAGS="$GOFLAGS -p=2" ;; esac
case " $GOFLAGS " in *" -trimpath "*) ;; *) GOFLAGS="$GOFLAGS -trimpath" ;; esac
uv run python scripts/operator_test_binary.py
# Compile the runtime fixture once before the short process-level timeouts.
(cd sdo/operator/runtime && go test -run '^$' .)
# Compile the bootstrap fixture and generated detector/controller dependencies.
(cd sdo/operator && go build -o "$root/.fastloop/test-bin/setup" "$root/tests/unit/benchmarks/sregym/fastloop/fixtures/setup.go")
uv run --extra test pytest -q --durations=15 -p no:tach -n 2 -m go_integration tests
