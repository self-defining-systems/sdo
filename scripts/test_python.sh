#!/bin/bash
# Run python tests using pytest via uv

set -e

# Get the directory of the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Running Python tests..."
cd "$PROJECT_ROOT"

# Parallelize with pytest-xdist; pytest-cov combines the per-worker
# coverage so the fail_under gate still sees the whole suite. Override with
# SDO_PYTEST_WORKERS (e.g. 0 to run in-process for a targeted local run).
WORKERS="${SDO_PYTEST_WORKERS:-6}"
uv run --extra test pytest -n "$WORKERS" --cov --cov-report=term-missing "$@" tests
