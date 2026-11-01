#!/bin/bash
# Run python tests using pytest via uv

set -e

# Get the directory of the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Running Python tests..."
cd "$PROJECT_ROOT"

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

# Parallelize across cores with pytest-xdist; pytest-cov combines the per-worker
# coverage so the fail_under gate still sees the whole suite. Override with
# SDO_PYTEST_WORKERS (e.g. 0 to run in-process for a targeted local run).
WORKERS="${SDO_PYTEST_WORKERS:-auto}"
uv run --extra test pytest -n "$WORKERS" --cov --cov-report=term-missing "$@" tests
