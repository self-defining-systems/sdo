#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

(
    cd "$root/sdo/operator/runtime"
    go test -run '^(TestHermeticFastloopRealGoGate|TestHermeticFakeConfigMapEnforcesProductionLimit|TestRegisteredReflectionWorkerRejectsOversizedResultBeforeCommit|TestHermeticResponderProgramReuseThroughRealToolTransport)$' -count=1 .
)

cd "$root"
uv run --extra test pytest -q -p no:tach tests/unit/benchmarks/sregym/fastloop/test_fastloop_hermetic_run.py
