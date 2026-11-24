#!/usr/bin/env bash
# Go static contracts run on the prepared graph, outside Python CI jobs.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# Stages that could not run here (toolchain unavailable) are listed at the end.
if [ -z "${SDO_SKIPPED_STAGES:-}" ]; then
    export SDO_SKIPPED_STAGES="$(mktemp)"
    trap 'rm -f "$SDO_SKIPPED_STAGES"' EXIT
    owns_skip_log=1
else
    owns_skip_log=0
fi
print_skipped_stages() {
    if [ "$owns_skip_log" = 1 ] && [ -s "$SDO_SKIPPED_STAGES" ]; then
        echo "==> SKIPPED stages (not checked locally; not a pass for these):" >&2
        sed 's/^/    - /' "$SDO_SKIPPED_STAGES" >&2
    fi
}

echo "==> GitHub Actions workflows"
bash "$SCRIPT_DIR/check_actions.sh"

# Local checks execute these tests here. CI prepares the same packages and
# runs the same selections/timeouts in its existing native contract jobs.
quality_mode=()
run_parity=1
case "${1:-}" in
    "") ;;
    --defer-prepared) quality_mode=(--defer-prepared) ;;
    # The hosted Go tests job already runs the contract modules and the
    # runtime proto-mirror tests; the checks job skips the second run.
    --skip-go-tests) run_parity=0; quality_mode=(--defer-prepared) ;;
    *) echo "unknown Go check mode: $1" >&2; exit 2 ;;
esac
echo "==> gofmt (all Go modules)"
bash "$SCRIPT_DIR/gofmt.sh" --check

echo "==> generated skill references"
bash "$SCRIPT_DIR/gen_skill_references.sh" --check
python3 "$SCRIPT_DIR/go_quality_checks.py" --group layout "${quality_mode[@]}"

echo "==> generated operator configuration reference (docs/config.md)"
bash "$SCRIPT_DIR/gen_config_docs.sh" --check

echo "==> generated read-only Kubernetes client facade"
bash "$SCRIPT_DIR/gen_kube_read_clients.sh" --check

# Proto contract drift (buf lint + committed-codegen diff). Enforced
# authoritatively by the dedicated `proto` CI job; skipped here when the buf
# toolchain is absent so a buf-less environment still passes the Python gates.
export PATH="${GOBIN:-${HOME}/go/bin}:${PATH}"
if command -v buf >/dev/null 2>&1; then
    echo "==> proto contracts (scripts/check_proto.sh)"
    proto_status=0
    "$SCRIPT_DIR/check_proto.sh" || proto_status=$?
    if [ "$proto_status" -eq 75 ]; then
        # The registry or pinned protoc is unavailable: say so loudly, keep going.
        echo "!!! SKIPPED: proto contracts (buf/BSR or pinned protoc unavailable); the proto CI job enforces them" >&2
        echo "proto contracts: buf/BSR or pinned protoc unavailable" >> "$SDO_SKIPPED_STAGES"
    elif [ "$proto_status" -ne 0 ]; then
        exit "$proto_status"
    fi
else
    echo "==> proto contracts: skipped (buf not installed; enforced by the proto CI job)"
fi

echo "==> Go nondeterminism ratchet"
"$SCRIPT_DIR/check_go_determinism.sh"

# Functional Go core: dependency/API lint plus structural purity checks.
"$SCRIPT_DIR/check_go_core.sh"

# Go programs under benchmarks/ must still compile against the operator API.
echo "==> go mains (benchmarks/)"
"$SCRIPT_DIR/check_go_mains.sh"

echo "==> lanes layering (what the lanes packages may name)"
"$SCRIPT_DIR/check_lanes_layering.sh"

if [[ "$run_parity" == 1 ]]; then
    "$SCRIPT_DIR/check_contracts_fast.sh" --go-only
fi

print_skipped_stages
