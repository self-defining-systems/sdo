#!/bin/bash
# Check for static errors.  Covers:
#   ruff  — syntax, undefined names, unused imports, style
#   tach  — architectural module boundary contracts (tach.toml)
#   arch  — AST facade imports and ownership rules (tests/unit/test_architecture.py)
#   types — pyright (scripts/type_check.sh)
#   go    — every Go main under benchmarks/ compiles (scripts/check_go_mains.sh)
#   schema — validator fingerprint and CLI binding contract tests

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

if ! command -v uv >/dev/null 2>&1; then
    echo "Error: 'uv' is not installed."
    exit 1
fi

# Validate workflow expressions before pushing a commit that cannot start CI.
echo "==> GitHub Actions workflows"
bash "$SCRIPT_DIR/check_actions.sh"

# Bind controller and validator to the same source identity and exercise an
# SDK branch merge without generated pin edits before the broader gates.
# Bound test execution with -timeout; cold module downloads and compilation need their own time.
echo "==> validator schema identity"
(
    cd "$PROJECT_ROOT/sdo/operator"
    go test ./builder ./operatorcli \
        -run '^(TestSchemaIdentity.*|TestSDKBranchesMergeWithoutIdentityPins|TestControllerSchemaIdentityFailsClosed|TestParseValidateLaunchRequiresBoundSchema)$' \
        -count=1 -timeout=20s
)

# Fixture Git must not leave detached writers behind TempDir cleanup.
echo "==> Go git fixture lifetime policy"
(
    cd "$PROJECT_ROOT/sdo/operator"
    go test ./internal/testutil -run '^TestGit(CreationPolicyNegativeControls|FixtureCreationUsesHelper)$' -count=1 -timeout=20s
)

echo "==> responder command reference"
uv run python -m sdo.agents.responder.command_reference --check

echo "==> ruff"
if [ "$1" == "--fix" ]; then
    uv run ruff check --fix .
else
    uv run ruff check .
fi

echo "==> tach (module boundaries)"
uv run tach check

echo "==> architecture (AST import and ownership rules)"
# These tests scan source files; Tach's import-based impact analysis cannot
# determine which changes affect them, so always run the complete file.
uv run --extra test python -m pytest -q -p no:tach tests/unit/test_architecture.py

echo "==> types (scripts/type_check.sh)"
"$SCRIPT_DIR/type_check.sh"

# Proto contract drift (buf lint + committed-codegen diff). Enforced
# authoritatively by the dedicated `proto` CI job; skipped here when the buf
# toolchain is absent so a buf-less environment still passes the Python gates.
export PATH="${GOBIN:-${HOME}/go/bin}:${PATH}"
if command -v buf >/dev/null 2>&1; then
    echo "==> proto contracts (scripts/check_proto.sh)"
    "$SCRIPT_DIR/check_proto.sh"
else
    echo "==> proto contracts: skipped (buf not installed; enforced by the proto CI job)"
fi

# Functional Go core: dependency/API lint plus structural purity checks.
"$SCRIPT_DIR/check_go_core.sh"

# Go programs under benchmarks/ must still compile against the operator API.
echo "==> go mains (benchmarks/)"
"$SCRIPT_DIR/check_go_mains.sh"
