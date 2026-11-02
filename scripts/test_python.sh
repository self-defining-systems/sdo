#!/bin/bash
# Run python tests using pytest via uv

set -e

# Get the directory of the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Running Python tests..."
cd "$PROJECT_ROOT"

# xdist workers swallow a missing-prerequisite error as an INTERNALERROR with no
# reason; fail here, once, with the remedy printed.
uv run python scripts/init_test_prereqs.py --check

# Build the benchmark protocol tools once for this invocation, from this checkout.
# xdist workers otherwise independently compile/link the same executables on their
# first receipt/submission test. Only compiler entries persist; never trust a
# restored executable from another source revision.
TOOLS="$(mktemp -d)"
trap 'rm -rf "$TOOLS"' EXIT
for spec in 'native_receipt_go sdo-native-receipt' 'submission_go sdo-submission-guard'; do
    read -r module binary <<< "$spec"
    (cd "benchmarks/sregym/protocol/$module" && go build -o "$TOOLS/$binary" "./cmd/$binary")
done
export SDO_NATIVE_RECEIPT_BIN="$TOOLS/sdo-native-receipt"
export SDO_SUBMISSION_GUARD_BIN="$TOOLS/sdo-submission-guard"

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
