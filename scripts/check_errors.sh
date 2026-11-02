#!/bin/bash
# Check for static errors.  Covers:
#   ruff  — syntax, undefined names, unused imports, style
#   tach  — architectural module boundary contracts (tach.toml)
#   arch  — AST facade imports and ownership rules (tests/unit/test_architecture.py)
#   types — pyright (scripts/type_check.sh)
#   pin   — third_party/sregym never moves backwards (scripts/check_submodule_pin.py)
#   go    — every Go main under benchmarks/ compiles (scripts/check_go_mains.sh)

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

if ! command -v uv >/dev/null 2>&1; then
    echo "Error: 'uv' is not installed."
    exit 1
fi

# Keep per-check wall time in hosted logs even while this script overlaps pytest.
check() {
    local started=$SECONDS
    "$@"
    echo "Timing: $*: $((SECONDS - started))s"
}

echo "==> ruff"
if [ "$1" == "--fix" ]; then
    check uv run ruff check --fix .
else
    check uv run ruff check .
fi

echo "==> tach (module boundaries)"
check uv run tach check

echo "==> architecture (AST import and ownership rules)"
# These tests scan source files; Tach's import-based impact analysis cannot
# determine which changes affect them. --python-suite delegates the complete
# file to the concurrently running full Python suite (without impact selection).
if [[ "${1:-}" != --python-suite ]]; then
    check uv run --extra test python -m pytest -q -p no:tach tests/unit/test_architecture.py
fi

echo "==> SREGym pin ancestry"
check uv run python scripts/check_submodule_pin.py

echo "==> types (scripts/type_check.sh)"
check "$SCRIPT_DIR/type_check.sh"

# Proto contract drift (buf lint + committed-codegen diff) runs in checks/Python.
# Skipped here when the buf
# toolchain is absent so a buf-less environment still passes the Python gates.
export PATH="${GOBIN:-${HOME}/go/bin}:${PATH}"
if command -v buf >/dev/null 2>&1; then
    echo "==> proto contracts (scripts/check_proto.sh)"
    check "$SCRIPT_DIR/check_proto.sh"
else
    echo "==> proto contracts: skipped (buf not installed; required in checks/Python CI)"
fi

echo "==> Go nondeterminism ratchet"
check "$SCRIPT_DIR/check_go_determinism.sh"

# Functional Go core: dependency/API lint plus structural purity checks.
check "$SCRIPT_DIR/check_go_core.sh"

# Build production packages without test declarations, then vet benchmark API consumers.
echo "==> Go production packages and benchmark programs"
check "$SCRIPT_DIR/check_go_mains.sh"
