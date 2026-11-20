#!/bin/bash
# Format Python and Go code using the locked ruff (default dev group).
# `python -m ruff` fails when ruff is missing from the project environment
# instead of falling back to a different ruff on PATH.

set -e

# Get the directory of the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

if [ "$1" == "--check" ]; then
    echo "Checking code formatting (dry-run)..."
    uv run python -m ruff format --check .
    bash "$SCRIPT_DIR/gofmt.sh" --check
else
    echo "Formatting code..."
    uv run python -m ruff format .
    bash "$SCRIPT_DIR/gofmt.sh"
fi

echo "Formatting check complete."
