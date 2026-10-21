#!/bin/bash
# Regenerate the Go and Python code from the proto contracts.
#
# proto/ is the single source of truth for SDO's cross-seam contracts
# Generated code is committed so builds
# do not need the toolchain; run this after editing any .proto and commit the
# diff. scripts/check_proto.sh asserts the committed output matches.

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# protoc-gen-go and (when installed via `go install`) buf live in ~/go/bin.
export PATH="${GOBIN:-${HOME}/go/bin}:${PATH}"

if ! command -v buf >/dev/null 2>&1; then
    echo "Error: 'buf' is not installed. Install it with:" >&2
    echo "  go install github.com/bufbuild/buf/cmd/buf@latest" >&2
    exit 1
fi
if ! command -v protoc-gen-go >/dev/null 2>&1; then
    echo "Error: 'protoc-gen-go' is not installed. Install it with:" >&2
    echo "  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest" >&2
    exit 1
fi

echo "==> buf lint"
buf lint

VALIDATE_TMP="$(mktemp -d)"
trap 'rm -rf "${VALIDATE_TMP}"' EXIT
if [ -d sdo/contracts/_gen/buf/validate ]; then
    cp -a sdo/contracts/_gen/buf/validate "${VALIDATE_TMP}/cached-validate"
fi

echo "==> buf generate"
buf generate

# Vendor the classic google.protobuf gencode of the buf.validate options our
# messages carry, so the generated Python modules are importable under the
# codebase's classic protobuf runtime (see buf.gen.validate.yaml). buf.gen.yaml
# uses clean:true, so this must run after `buf generate`.
echo "==> vendor buf.validate python gencode"
if buf generate --include-imports --template buf.gen.validate.yaml -o "${VALIDATE_TMP}" buf.build/bufbuild/protovalidate 2>"${VALIDATE_TMP}/error"; then
    rm -rf sdo/contracts/_gen/buf
    cp -r "${VALIDATE_TMP}/_gen_validate/buf" sdo/contracts/_gen/buf
elif grep -qi 'resource_exhausted: too many requests' "${VALIDATE_TMP}/error" && [ -d "${VALIDATE_TMP}/cached-validate" ]; then
    echo "   using unchanged cached buf.validate gencode after BSR rate limit" >&2
    mkdir -p sdo/contracts/_gen/buf
    cp -r "${VALIDATE_TMP}/cached-validate" sdo/contracts/_gen/buf/validate
else
    cat "${VALIDATE_TMP}/error" >&2
    exit 1
fi

echo "Proto code regenerated."
