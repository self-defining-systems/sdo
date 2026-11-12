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
#   budgets — per-commit prompt-budget policy against the merge base (CI: required-pr-ci)
#   determinism — no production real time, process or socket use outside declared edge packages

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"
source "$PROJECT_ROOT/scripts/isolate_git_config.sh"

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

echo "==> tracked experiment outputs"
uv run python scripts/check_experiment_outputs.py

mode="${1:-}"
case "$mode" in
    --go-only) exec bash "$SCRIPT_DIR/check_go_errors.sh" "${@:2}" ;;
    ""|--fix) check bash "$SCRIPT_DIR/check_go_errors.sh" ;;
    # The Go tests job runs the contract parity tests; skip them here.
    --python-suite) check bash "$SCRIPT_DIR/check_go_errors.sh" --skip-go-tests ;;
    --python-only) ;;
    *) echo "unknown check mode: $mode" >&2; exit 2 ;;
esac


echo "==> pytest import names"
uv run python scripts/check_pytest_imports.py

# Source-scanning invariants must run even when focused tests omit their package.
# Keep this selection free of integration tests and long property-test sweeps.
# --python-suite delegates them to the concurrently running full Python suite,
# which contains every file named here.
if [[ "$mode" != --python-suite ]]; then
echo "==> Python fast repository ratchets"
uv run --extra test python -m pytest -q -p no:tach \
    tests/unit/test_polling_policy.py \
    tests/unit/scripts/test_check_experiment_outputs.py \
    tests/unit/benchmarks/sregym/fastloop/test_image_provenance.py::test_standalone_provenance_uses_shared_git_policy_without_site_packages \
    tests/unit/benchmarks/sregym/adapter/test_budgets.py::test_image_bound_files_do_not_import_the_budgets \
    tests/unit/benchmarks/sregym/fastloop/test_shared_git_ownership.py::test_unscoped_host_git_negative_controls \
    tests/unit/benchmarks/sregym/fastloop/test_shared_git_ownership.py::test_all_benchmark_host_checkout_commands_use_the_scoped_policy \
    tests/unit/scripts/test_check_prompt_budgets.py \
    tests/unit/scripts/test_check_app_names.py \
    tests/unit/scripts/test_ci_quarantine.py \
    tests/unit/benchmarks/sregym/adapter/test_budgets.py::test_the_scanner_sees_each_wait_shape \
    tests/unit/benchmarks/sregym/adapter/test_budgets.py::test_seconds_literals_in_waits_only_decrease \
    tests/unit/sdo/agents/test_prompt_generalization.py \
    tests/unit/sdo/agents/prompts/test_context_registry.py \
    tests/unit/sdo/agents/prompts/test_required_content.py \
    tests/unit/sdo/agents/prompts/test_golden.py::test_prompt_goldens \
    tests/unit/sdo/agents/responder/test_tools_cli.py::test_local_help_is_a_concise_capability_index \
    tests/unit/sdo/agents/responder/test_codex_responder.py::test_workflow_prefers_finishing_blocking_operations_to_model_polling \
    tests/unit/sdo/agents/responder/test_incident_response_workflow.py::test_incident_workflow_installs_from_the_shipped_skill_root
fi

# Pull-request CI checks this per commit in its final job; run it here against the merge base.
if [[ "$mode" != --python-suite ]] && base="$(git merge-base origin/main HEAD 2>/dev/null)"; then
    echo "==> prompt budget commit policy"
    uv run python scripts/check_prompt_budgets.py --base "$base" --head HEAD
fi

echo "==> shipped skill size budgets"
uv run python scripts/check_skill_sizes.py
uv run python scripts/check_app_names.py

echo "==> flaky-test quarantine deadlines"
uv run python scripts/ci_quarantine.py check

echo "==> responder skill contracts"
uv run python -m sdo.agents.responder.skill_contracts --check

# A merge that keeps this branch's older SREGym pin silently reverts the base
# branch's pin; the base pin must be an ancestor of (or equal to) this one.
echo "==> SREGym submodule pin"
uv run python scripts/check_submodule_pin.py

echo "==> ruff"
if [ "$mode" == "--fix" ]; then
    check uv run python -m ruff check --fix .
    check uv run python -m ruff format .
else
    check uv run python -m ruff check .
    # Same check as hosted CI: a branch that needs formatting fails here, so
    # format_code.sh never rewrites files the branch did not touch.
    check uv run python -m ruff format --check .
fi

echo "==> tach (module boundaries)"
check uv run tach check

echo "==> architecture (AST import and ownership rules)"
# These tests scan source files; Tach's import-based impact analysis cannot
# determine which changes affect them. --python-suite delegates the complete
# file to the concurrently running full Python suite (without impact selection).
if [[ "$mode" != --python-suite ]]; then
    check uv run --extra test python -m pytest -q -p no:tach tests/unit/test_architecture.py
fi

echo "==> types (scripts/type_check.sh)"
check "$SCRIPT_DIR/type_check.sh"

# tests/unit/sdo/contracts is part of the full Python suite that --python-suite runs.
if [[ "$mode" != --python-suite ]]; then
    check "$SCRIPT_DIR/check_contracts_fast.sh" --python-only
fi
