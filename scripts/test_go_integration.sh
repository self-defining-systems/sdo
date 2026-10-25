#!/usr/bin/env bash
# External-process contracts execute after compilation, outside their timeouts.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
export GOMAXPROCS="${GOMAXPROCS:-2}"
export GOFLAGS="${GOFLAGS:-} -p=2"
uv run python scripts/operator_test_binary.py
# Compile the runtime fixture once before the short process-level timeouts.
(cd sdo/operator/runtime && go test -run '^$' .)
# Compile the bootstrap fixture and generated detector/controller dependencies.
(cd sdo/operator && go build -o "$root/.fastloop/test-bin/setup" "$root/tests/unit/benchmarks/sregym/fastloop/fixtures/setup.go")
uv run --extra test pytest -q -p no:tach -n 0 -m go_integration tests
