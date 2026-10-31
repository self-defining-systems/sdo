#!/bin/bash
# Static checks for the proto contracts:
#   buf lint       — style and consistency of the .proto source
#   generated diff  — checked-in Go/Python matches what buf generate emits
#
# The generated-diff check is the ratchet: a .proto edit that is not accompanied
# by its regenerated code (or a hand-edit of generated code) fails here, so drift
# is a build failure, not a 10-minutes-into-a-run failure. Compare snapshots so
# this check also works before a refactor's generated files are staged in Git.

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

export PATH="${GOBIN:-${HOME}/go/bin}:${PATH}"

if ! command -v buf >/dev/null 2>&1; then
    echo "Error: 'buf' is not installed (go install github.com/bufbuild/buf/cmd/buf@latest)." >&2
    exit 1
fi

echo "==> buf lint"
timeout 30 buf lint

echo "==> generated code is up to date"
GEN_DIRS=(sdo/operator/contracts/gen sdo/contracts/_gen)
# Generate in a private workspace: Buf's clean:true deletes output trees.
# A read-only drift check must not remove live Go/Python compiler inputs.
GENERATION_ROOT="$(mktemp -d)"
trap 'rm -rf "${GENERATION_ROOT}"' EXIT
mkdir -p "${GENERATION_ROOT}/scripts" "${GENERATION_ROOT}/sdo/contracts"
cp "$SCRIPT_DIR/gen_proto.sh" "${GENERATION_ROOT}/scripts/"
cp buf.yaml buf.lock buf.gen.yaml buf.gen.validate.yaml "${GENERATION_ROOT}/"
cp -a sdo/contracts/proto "${GENERATION_ROOT}/sdo/contracts/"
for generated_dir in "${GEN_DIRS[@]}"; do
    mkdir -p "${GENERATION_ROOT}/$(dirname "${generated_dir}")"
    cp -a "${generated_dir}" "${GENERATION_ROOT}/${generated_dir}"
done
"${GENERATION_ROOT}/scripts/gen_proto.sh" >/dev/null
if ! diff -qr --exclude='__pycache__' "${GENERATION_ROOT}/sdo/operator/contracts/gen" sdo/operator/contracts/gen >/dev/null \
    || ! diff -qr --exclude='__pycache__' "${GENERATION_ROOT}/sdo/contracts/_gen" sdo/contracts/_gen >/dev/null; then
    echo "Generated proto code is out of date. Run scripts/gen_proto.sh:" >&2
    exit 1
fi

echo "Proto checks passed."
