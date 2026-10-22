#!/bin/bash
# Check architecture rules using tach
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Checking architecture rules..."
cd "$PROJECT_ROOT"

if command -v uv >/dev/null 2>&1; then
    uv run tach check
else
    echo "Error: 'uv' is not installed."
    exit 1
fi

# Functional Go core: dependency/API lint plus structural purity checks.
"$SCRIPT_DIR/check_go_core.sh"
