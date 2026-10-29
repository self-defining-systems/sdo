#!/usr/bin/env bash
# Go static contracts run on the prepared graph, outside Python CI jobs.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

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

# Tests must wait for the condition they assert, not race Execute against a
# fixed sub-second deadline (flaky under -race and host load).
echo "==> Go Execute deadline policy"
(
    cd "$PROJECT_ROOT/sdo/operator"
    go test ./internal/testutil -run '^Test(ExecuteDeadlinePolicyNegativeControls|NoFixedShortDeadlineAroundExecuteInTests|CountedPollingPolicyNegativeControls|CountedPollingAllowances|NoShortCountedPollingInTests)$' -count=1 -timeout=20s
)

# Capture fixtures must come from the real CaptureSnapshot projection.
echo "==> Go capture fixture policy"
(
    cd "$PROJECT_ROOT/sdo/operator"
    go test ./internal/capturefixture -count=1 -timeout=20s
)

# Generated skill references: the root sdk package test only runs when that
# package changes, so an sdk/program-only edit can leave program-sdk.md stale.
echo "==> generated skill references"
bash "$SCRIPT_DIR/gen_skill_references.sh" --check
(
    cd "$PROJECT_ROOT/sdo/operator"
    go test ./memory/validate -run '^TestMemoryLayoutReference$' -count=1 -timeout=20s
)

echo "==> generated read-only Kubernetes client facade"
bash "$SCRIPT_DIR/gen_kube_read_clients.sh" --check

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

# Cross-language parity (Go constants, Python models, protos) in seconds.
"$SCRIPT_DIR/check_contracts_fast.sh" --go-only

# Functional Go core: dependency/API lint plus structural purity checks.
"$SCRIPT_DIR/check_go_core.sh"

# Go programs under benchmarks/ must still compile against the operator API.
echo "==> go mains (benchmarks/)"
"$SCRIPT_DIR/check_go_mains.sh"
