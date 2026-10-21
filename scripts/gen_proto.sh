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
# Buf clean:true can remove generated files before a remote plugin fails.
# Restore the original trees on any failure, including a failed local retry.
GEN_DIRS=(sdo/operator/contracts/gen sdo/contracts/_gen)
for generated_dir in "${GEN_DIRS[@]}"; do
    mkdir -p "${VALIDATE_TMP}/original/$(dirname "${generated_dir}")"
    cp -a "${generated_dir}" "${VALIDATE_TMP}/original/${generated_dir}"
done
GEN_COMPLETE=false
cleanup() {
    if [ "$GEN_COMPLETE" != true ]; then
        for generated_dir in "${GEN_DIRS[@]}"; do
            rm -rf "${generated_dir}"
            cp -a "${VALIDATE_TMP}/original/${generated_dir}" "${generated_dir}"
        done
    fi
    rm -rf "${VALIDATE_TMP}"
}
trap cleanup EXIT
if [ -d sdo/contracts/_gen/buf/validate ]; then
    cp -a sdo/contracts/_gen/buf/validate "${VALIDATE_TMP}/cached-validate"
fi

echo "==> buf generate"
if ! timeout 240 buf generate 2>"${VALIDATE_TMP}/generate-error"; then
    if ! grep -qi 'resource_exhausted' "${VALIDATE_TMP}/generate-error"; then
        cat "${VALIDATE_TMP}/generate-error" >&2
        exit 1
    fi
    if ! command -v protoc >/dev/null 2>&1 || [ "$(protoc --version)" != "libprotoc 33.4" ]; then
        echo "BSR rate limited: local fallback requires protoc 33.4 on PATH." >&2
        exit 1
    fi
    echo "   BSR rate limited; generating locally with pinned protoc 33.4" >&2
    # Preserve the managed Go generation and replace only the remote plugins.
    sed -e 's|remote: buf.build/protocolbuffers/python:v33.4|protoc_builtin: python|' \
        -e 's|remote: buf.build/protocolbuffers/pyi:v33.4|protoc_builtin: pyi|' \
        buf.gen.yaml > "${VALIDATE_TMP}/local.yaml"
    for generated_dir in "${GEN_DIRS[@]}"; do
        rm -rf "${generated_dir}"
        cp -a "${VALIDATE_TMP}/original/${generated_dir}" "${generated_dir}"
    done
    timeout 240 buf generate --template "${VALIDATE_TMP}/local.yaml"
fi

# Vendor the classic google.protobuf gencode of the buf.validate options our
# messages carry, so the generated Python modules are importable under the
# codebase's classic protobuf runtime (see buf.gen.validate.yaml). buf.gen.yaml
# uses clean:true, so this must run after `buf generate`.
echo "==> vendor buf.validate python gencode"
if timeout 60 buf generate --include-imports --template buf.gen.validate.yaml -o "${VALIDATE_TMP}" buf.build/bufbuild/protovalidate 2>"${VALIDATE_TMP}/error"; then
    rm -rf sdo/contracts/_gen/buf
    cp -r "${VALIDATE_TMP}/_gen_validate/buf" sdo/contracts/_gen/buf
elif grep -qi 'resource_exhausted' "${VALIDATE_TMP}/error" && [ -d "${VALIDATE_TMP}/cached-validate" ]; then
    echo "   using unchanged cached buf.validate gencode after BSR rate limit" >&2
    mkdir -p sdo/contracts/_gen/buf
    cp -r "${VALIDATE_TMP}/cached-validate" sdo/contracts/_gen/buf/validate
else
    cat "${VALIDATE_TMP}/error" >&2
    exit 1
fi

GEN_COMPLETE=true
echo "Proto code regenerated."
