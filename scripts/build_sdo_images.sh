#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tag="${SDO_IMAGE_TAG:-v0.1.0}"
# CI builds only changed targets through Bake, then uses these same smoke gates.
read -r -a selected <<<"${SDO_IMAGES:-controller validator responder benchmark}"
for image in "${selected[@]}"; do
  case "${image}" in
  controller | validator | responder | benchmark) ;;
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
  # The images share no layers beyond the cached Go dependency stage, so build
  # them concurrently; the benchmark worker extends the responder and follows it.
  pids=()
  if selected_image validator; then
    docker build \
      --load \
      --file "${repo_root}/sdo/operator/Dockerfile.validator" \
      --build-arg "SDO_VALIDATOR_SCHEMA_IDENTITY=${validator_schema_identity}" \
      --tag sdo-detector-validator:${tag} \
      "${repo_root}" &
    pids+=($!)
  fi
  if selected_image controller; then
    docker build \
      --load \
      --file "${repo_root}/sdo/operator/Dockerfile.runtime" \
      --target controller \
      --tag sdo-controller:${tag} \
      "${repo_root}" &
    pids+=($!)
  fi
  if selected_image responder || selected_image benchmark; then
    (
      docker build \
        --load \
        --file "${repo_root}/sdo/operator/Dockerfile.runtime" \
        --target responder \
        --tag sdo-responder:${tag} \
        "${repo_root}"
      if selected_image benchmark; then
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
      fi
    ) &
    pids+=($!)
  fi
  build_failed=0
  for pid in "${pids[@]}"; do
    wait "${pid}" || build_failed=1
  done
  if [[ "${build_failed}" != "0" ]]; then
    echo "image build failed" >&2
    exit 1
  fi
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
  benchmark)
    image=sdo-sregym-responder
    role=responder_image
    ;;
  esac
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
  if [[ "${SDO_IMAGE_PREFLIGHT:-0}" == "1" ]]; then
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
