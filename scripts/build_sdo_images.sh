#!/usr/bin/env bash
set -euo pipefail

# SDO_IMAGE_TAG selects a private tag (for example lp1) so a build never retags the shared v0.1.0 images.
tag="${SDO_IMAGE_TAG:-v0.1.0}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# SDO_IMAGE_TAG builds under a private tag so shared v0.1.0 images stay untouched.
tag="${SDO_IMAGE_TAG:-v0.1.0}"
# Compute the validator schema identity from the sources baked into the image.
validator_schema_identity="$(cd "${repo_root}/sdo/operator" && go run ./cmd/sdo-operator schema-identity)"
docker build \
  --load \
  --file "${repo_root}/sdo/operator/Dockerfile.validator" \
  --build-arg "SDO_VALIDATOR_SCHEMA_IDENTITY=${validator_schema_identity}" \
  --tag sdo-detector-validator:${tag} \
  "${repo_root}"
docker build \
  --load \
  --file "${repo_root}/sdo/operator/Dockerfile.runtime" \
  --target controller \
  --tag sdo-controller:${tag} \
  "${repo_root}"
docker build \
  --load \
  --file "${repo_root}/sdo/operator/Dockerfile.runtime" \
  --target responder \
  --tag sdo-responder:${tag} \
  "${repo_root}"
docker build \
  --load \
  --file "${repo_root}/sdo/operator/Dockerfile.runtime" \
  --target sregym-responder \
  --tag sdo-sregym-responder:${tag} \
  "${repo_root}"
for image in sdo-detector-validator sdo-controller sdo-responder sdo-sregym-responder; do
  docker run --rm --user 65532:65532 "${image}:${tag}" sh -ec '
    command -v go >/dev/null
    command -v sdo-operator >/dev/null
    test "$GOPROXY" = off
    test "$GOFLAGS" = "-mod=mod -p=2"
    cd /opt/sdo/sdo/operator/sdk
    go list ./program >/dev/null
  '
  docker run --rm --user 65532:65532 \
    --volume "${repo_root}/tests/fixtures/sdo/playbook_program_app:/fixture:ro" \
    "${image}:${tag}" \
    sdo-operator validate --app /fixture --expect-schema "${validator_schema_identity}"
done
docker run --rm --user 65532:65532 \
  sdo-sregym-responder:${tag} \
  python3 -m benchmarks.sregym.adapter.submission --help >/dev/null
docker run --rm --user 65532:65532 \
  sdo-sregym-responder:${tag} \
  python3 -c "from sdo.agents.responder.codex import execute_incident" >/dev/null
