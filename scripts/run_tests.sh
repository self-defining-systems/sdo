#!/bin/bash
# Run the complete Python and Go test suites.

set -e

# Get the directory of the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Running all tests..."

# Run Python tests
"$SCRIPT_DIR/test_python.sh" "$@"

for module in sdo/operator sdo/operator/contracts sdo/operator/sdk sdo/operator/core sdo/operator/runtime; do
  (cd "$PROJECT_ROOT/$module" && go test ./...)
done
