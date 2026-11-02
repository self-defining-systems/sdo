#!/bin/bash
# Check for static errors.  Covers:
#   ruff  — syntax, undefined names, unused imports, style
#   tach  — architectural module boundary contracts (tach.toml)
#   arch  — AST facade imports and ownership rules (tests/unit/test_architecture.py)
#   types — pyright (scripts/type_check.sh)
#   go    — every Go main under benchmarks/ compiles (scripts/check_go_mains.sh)
#   deadline — no short Execute deadlines or counted sleep-poll budgets in tests
#   schema — validator fingerprint and CLI binding contract tests
#   parity — fast Go/Python/proto contract parity (scripts/check_contracts_fast.sh)
#   pin    — the SREGym gitlink never moves behind the base branch
#   refs   — generated skill references and the read-only Kubernetes client facade

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

if ! command -v uv >/dev/null 2>&1; then
    echo "Error: 'uv' is not installed."
    exit 1
fi

<<<<<<< HEAD
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
=======
mode="${1:-}"
case "$mode" in
    --go-only) exec bash "$SCRIPT_DIR/check_go_errors.sh" "${@:2}" ;;
    ""|--fix) bash "$SCRIPT_DIR/check_go_errors.sh" ;;
    --python-only) ;;
    *) echo "unknown check mode: $mode" >&2; exit 2 ;;
esac

# Validate workflow expressions before pushing a commit that cannot start CI.
uv run python "$SCRIPT_DIR/check_ci_budget.py"

echo "==> pytest import names"
uv run python scripts/check_pytest_imports.py

# Source-scanning invariants must run even when focused tests omit their package.
# Keep this selection free of integration tests and long property-test sweeps.
echo "==> Python fast repository ratchets"
uv run --extra test python -m pytest -q -p no:tach \
    tests/unit/test_polling_policy.py \
    tests/unit/benchmarks/sregym/fastloop/test_image_provenance.py::test_standalone_provenance_uses_shared_git_policy_without_site_packages \
    tests/unit/benchmarks/sregym/adapter/test_budgets.py::test_image_bound_files_do_not_import_the_budgets \
    tests/unit/benchmarks/sregym/fastloop/test_shared_git_ownership.py::test_unscoped_host_git_negative_controls \
    tests/unit/benchmarks/sregym/fastloop/test_shared_git_ownership.py::test_all_benchmark_host_checkout_commands_use_the_scoped_policy \
    tests/unit/scripts/test_check_prompt_budgets.py \
    tests/unit/benchmarks/sregym/adapter/test_budgets.py::test_the_scanner_sees_each_wait_shape \
    tests/unit/benchmarks/sregym/adapter/test_budgets.py::test_seconds_literals_in_waits_only_decrease \
    tests/unit/sdo/agents/test_prompt_generalization.py \
    tests/unit/sdo/agents/prompts/test_context_registry.py \
    tests/unit/sdo/agents/prompts/test_required_content.py \
    tests/unit/sdo/agents/responder/test_incident_context.py::test_many_stage_attempts_cannot_displace_bounded_first_prompt

echo "==> shipped skill size budgets"
uv run python scripts/check_skill_sizes.py

echo "==> responder command reference"
uv run python -m sdo.agents.responder.command_reference --check

# A merge that keeps this branch's older SREGym pin silently reverts the base
# branch's pin; the base pin must be an ancestor of (or equal to) this one.
echo "==> SREGym submodule pin"
uv run python scripts/check_submodule_pin.py

echo "==> ruff"
if [ "$mode" == "--fix" ]; then
    uv run python -m ruff check --fix .
    uv run python -m ruff format .
else
    uv run python -m ruff check .
    # Same check as hosted CI: a branch that needs formatting fails here, so
    # format_code.sh never rewrites files the branch did not touch.
    uv run python -m ruff format --check .
>>>>>>> origin/vic-lsh/dev/sdo-improve
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

echo "==> types (scripts/type_check.sh)"
check "$SCRIPT_DIR/type_check.sh"

<<<<<<< HEAD
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
=======
"$SCRIPT_DIR/check_contracts_fast.sh" --python-only
>>>>>>> origin/vic-lsh/dev/sdo-improve
