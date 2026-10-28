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
# Preparation owns fixture compilation. Consumers verify the source-bound CLI
# and execute exactly the node IDs from the same-run cost plan.
case "${1:-all}" in
  all) uv run --extra test python -m pytest -v --durations=0 -p no:tach -n 2 -m go_integration tests ;;
  *) uv run --extra test python scripts/python_cost_plan.py --bin "$1" --plan "${2:-.fastloop/test-bin/python-test-plan.json}" ;;
esac
