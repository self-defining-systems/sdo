#!/bin/bash
# Run the complete Python and Go test suites.

set -e

# Get the directory of the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Running all tests..."

# Run Python tests
"$SCRIPT_DIR/test_python.sh" "$@"

# Every nested operator module is discovered from the tree, so a new one is never skipped.
operator_modules=()
for gomod in "$PROJECT_ROOT"/sdo/operator/*/go.mod; do
  operator_modules+=("sdo/operator/$(basename "$(dirname "$gomod")")")
done
for module in sdo/operator "${operator_modules[@]}" benchmarks/sregym/fastloop/replay_go; do
  (cd "$PROJECT_ROOT/$module" && go test -tags=integration ./...)
done
