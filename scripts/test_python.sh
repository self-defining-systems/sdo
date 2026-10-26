#!/bin/bash
# Run python tests using pytest via uv

set -e

# Get the directory of the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Running Python tests..."
cd "$PROJECT_ROOT"

# Bound shared-host parallelism; pytest-cov combines the per-worker
# coverage so the fail_under gate still sees the whole suite. Override with
# SDO_PYTEST_WORKERS (e.g. 0 to run in-process for a targeted local run).
if [[ -v SDO_PYTEST_WORKERS ]]; then
    WORKERS="$SDO_PYTEST_WORKERS"
else
    WORKERS="$(nproc)"
    if (( WORKERS > 4 )); then
        WORKERS=4
    fi
fi
# The two hosted Python runners share the suite without adding another job.
# Every path outside fastloop belongs to core, including new test directories.
# Shards publish raw data; Required PR CI combines it before enforcing coverage.
paths=(tests)
coverage=(--cov --cov-report=term-missing)
case "${SDO_PYTEST_SHARD:-}" in
    "") ;;
    core)
        paths=(--ignore=tests/unit/benchmarks/sregym/fastloop tests)
        coverage=(--cov --cov-report= --cov-fail-under=0)
        ;;
    fastloop)
        paths=(tests/unit/benchmarks/sregym/fastloop)
        coverage=(--cov --cov-report= --cov-fail-under=0)
        ;;
    *) echo "unknown Python test shard: $SDO_PYTEST_SHARD" >&2; exit 2 ;;
esac
uv run --extra test pytest -n "$WORKERS" "${coverage[@]}" "$@" "${paths[@]}"
