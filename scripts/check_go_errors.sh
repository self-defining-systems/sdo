#!/usr/bin/env bash
# Go static contracts run on the prepared graph, outside Python CI jobs.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

echo "==> GitHub Actions workflows"
bash "$SCRIPT_DIR/check_actions.sh"

# Local checks execute these tests here. CI prepares the same packages and
# runs the same selections/timeouts in its existing native contract jobs.
quality_mode=()
case "${1:-}" in
    "") ;;
    --defer-prepared) quality_mode=(--defer-prepared) ;;
    *) echo "unknown Go check mode: $1" >&2; exit 2 ;;
esac
for group in schema git deadlines capture; do
    python3 "$SCRIPT_DIR/go_quality_checks.py" --group "$group" "${quality_mode[@]}"
done

echo "==> generated skill references"
bash "$SCRIPT_DIR/gen_skill_references.sh" --check
python3 "$SCRIPT_DIR/go_quality_checks.py" --group layout "${quality_mode[@]}"

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

echo "==> Go nondeterminism ratchet"
"$SCRIPT_DIR/check_go_determinism.sh"

# Functional Go core: dependency/API lint plus structural purity checks.
"$SCRIPT_DIR/check_go_core.sh"

# Go programs under benchmarks/ must still compile against the operator API.
echo "==> go mains (benchmarks/)"
"$SCRIPT_DIR/check_go_mains.sh"

# Runtime parity tests need the full runtime test archive. Wait for its
# independent compiler instead of starting a second cold compile of it.
if [[ -n "${SDO_GO_RUNTIME_READY:-}" ]]; then
    read -r -t 230 ready < "$SDO_GO_RUNTIME_READY"
fi
"$SCRIPT_DIR/check_contracts_fast.sh" --go-only
