#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tag="${SDO_IMAGE_TAG:-v0.1.0}"
# CI builds only changed targets through Bake, then uses these same smoke gates.
read -r -a selected <<<"${SDO_IMAGES:-controller validator responder benchmark script-tools}"
for image in "${selected[@]}"; do
  case "${image}" in
  controller | validator | responder | benchmark | script-tools) ;;
  *)
    echo "unknown SDO image target: ${image}" >&2
    exit 1
    ;;
  esac
done
selected_image() { [[ " ${selected[*]} " == *" $1 "* ]]; }
validator_schema_identity="${SDO_VALIDATOR_SCHEMA_IDENTITY:-}"
if [[ -z "${validator_schema_identity}" ]]; then
  validator_schema_identity="$(cd "${repo_root}/sdo/operator" && go run ./cmd/sdo-operator schema-identity)"
fi
if [[ "${SDO_SKIP_IMAGE_BUILD:-0}" != "1" ]]; then
  export SDO_IMAGE_TAG="${tag}" SDO_VALIDATOR_SCHEMA_IDENTITY="${validator_schema_identity}"
  export SDO_WORKER_TARGET=sregym-responder
  if [[ "${SDO_SCRIPTED_WORKER:-0}" == "1" ]]; then
    export SDO_WORKER_TARGET=scripted-worker
  fi
  (cd "${repo_root}" && docker buildx bake --file scripts/sdo-images.hcl --load "${selected[@]}")
fi
for target in "${selected[@]}"; do
  case "${target}" in
  controller)
    image=sdo-controller
    role=controller_image
    ;;
  validator)
    image=sdo-detector-validator
    role=validator_image
    ;;
  responder)
    image=sdo-responder
    role=responder_image
    ;;
  script-tools)
    image=sdo-script-tools
    role=
    ;;
  benchmark)
    image=sdo-sregym-responder
    role=responder_image
    ;;
  esac
  if [[ "${target}" == "script-tools" ]]; then
    docker run --rm --user 65532:65532 "${image}:${tag}" sh -ec '
      for tool in bash kubectl jq curl tar sdo-operator; do command -v "$tool" >/dev/null; done
      ! command -v go
    '
  elif [[ "${target}" == "controller" ]]; then
    docker run --rm --user 65532:65532 "${image}:${tag}" sh -ec '
      command -v go >/dev/null
      test "$GOPROXY" = off
      test "$GOFLAGS" = "-mod=mod -p=2"
      cd /opt/sdo/sdo/operator/sdk
      go list ./program >/dev/null
    '
    docker run --rm --user 65532:65532 \
      --volume "${repo_root}/tests/fixtures/sdo/playbook_program_app:/fixture:ro" \
      "${image}:${tag}" sdo-operator validate --app /fixture --expect-schema "${validator_schema_identity}"
  else
    docker run --rm --user 65532:65532 "${image}:${tag}" sh -ec '
      ! command -v go
      command -v sdo-operator >/dev/null
      sdo-operator memory help >/dev/null
    '
  fi
  if [[ "${SDO_IMAGE_PREFLIGHT:-0}" == "1" && -n "${role}" ]]; then
    uv run python -m benchmarks.sregym.runner.preflight --image "${role}=${image}:${tag}"
  fi
  if [[ "${target}" == "responder" || "${target}" == "benchmark" ]]; then
    docker run --rm --user 65532:65532 \
      "${image}:${tag}" \
      python3 -c "from sdo.agents.responder import run_worker_task" >/dev/null
  fi
done
if selected_image benchmark; then
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
fi
