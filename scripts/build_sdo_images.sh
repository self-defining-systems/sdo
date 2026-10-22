#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tag="${SDO_IMAGE_TAG:-v0.1.0}"
# Compute the validator schema identity from the sources baked into the image.
validator_schema_identity="$(cd "${repo_root}/sdo/operator" && go run ./cmd/sdo-operator schema-identity)"
# The images share no layers beyond the cached Go dependency stage, so build
# them concurrently; the benchmark worker extends the responder and follows it.
pids=()
docker build \
  --load \
  --file "${repo_root}/sdo/operator/Dockerfile.validator" \
  --build-arg "SDO_VALIDATOR_SCHEMA_IDENTITY=${validator_schema_identity}" \
  --tag sdo-detector-validator:${tag} \
  "${repo_root}" &
pids+=($!)
docker build \
  --load \
  --file "${repo_root}/sdo/operator/Dockerfile.runtime" \
  --target controller \
  --tag sdo-controller:${tag} \
  "${repo_root}" &
pids+=($!)
(
  docker build \
    --load \
    --file "${repo_root}/sdo/operator/Dockerfile.runtime" \
    --target responder \
    --tag sdo-responder:${tag} \
    "${repo_root}"
  if [[ "${SDO_SCRIPTED_WORKER:-0}" == "1" ]]; then
    worker_target=scripted-worker
  else
    worker_target=sregym-responder
  fi
  docker build \
    --load \
    --build-arg "SDO_RESPONDER_IMAGE=sdo-responder:${tag}" \
    --file "${repo_root}/benchmarks/sregym/Dockerfile.worker" \
    --target "${worker_target}" \
    --tag sdo-sregym-responder:${tag} \
    "${repo_root}"
) &
pids+=($!)
build_failed=0
for pid in "${pids[@]}"; do
  wait "${pid}" || build_failed=1
done
if [[ "${build_failed}" != "0" ]]; then
  echo "image build failed" >&2
  exit 1
fi
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
for image in sdo-responder sdo-sregym-responder; do
  docker run --rm --user 65532:65532 \
    "${image}:${tag}" \
    python3 -c "from sdo.agents.responder import run_worker_task" >/dev/null
done
docker run --rm --user 65532:65532 \
  sdo-sregym-responder:${tag} \
  python3 -m benchmarks.sregym.adapter.submission --help >/dev/null
docker run --rm --user 65532:65532 \
  sdo-sregym-responder:${tag} \
  python3 -c "from sdo.agents.responder.codex import execute_incident" >/dev/null
if [[ "${SDO_SCRIPTED_WORKER:-0}" == "1" ]]; then
  docker run --rm --user 65532:65532 \
    sdo-sregym-responder:${tag} \
    python3 -c "from benchmarks.sregym.adapter.scripted_worker import scripted_responder, ScriptedReflector" >/dev/null
fi
