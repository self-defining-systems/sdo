#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tag="${SDO_IMAGE_TAG:-v0.1.0}"
# Provenance must describe the bytes Bake consumes, including untracked sources.
source_changes="$(git -C "${repo_root}" status --porcelain --untracked-files=normal -- sdo/operator scripts/build_sdo_images.sh scripts/sdo-images.hcl)"
if [[ -n "${source_changes}" ]]; then
  echo "image source has uncommitted changes; commit before building source-bound images" >&2
  exit 1
fi
export SDO_SOURCE_REVISION="$(git -C "${repo_root}" rev-parse HEAD)"
# Source-checked images also carry the tree identity of exactly the paths their
# Dockerfile stages read, so commits outside those paths reuse them.
provenance="${repo_root}/benchmarks/sregym/fastloop/image_provenance.py"
for target in controller validator; do
  built_paths="$(cd "${repo_root}" && python3 "${provenance}" paths "${target}" HEAD)"
  # shellcheck disable=SC2086 # one repository-relative path per line
  built_changes="$(git -C "${repo_root}" status --porcelain --untracked-files=normal -- ${built_paths})"
  if [[ -n "${built_changes}" ]]; then
    echo "image source has uncommitted changes; commit before building source-bound images" >&2
    exit 1
  fi
done
export SDO_CONTROLLER_SOURCE_TREE="$(cd "${repo_root}" && python3 "${provenance}" identity controller HEAD)"
export SDO_VALIDATOR_SOURCE_TREE="$(cd "${repo_root}" && python3 "${provenance}" identity validator HEAD)"
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
  elif [[ "${target}" == "controller" || "${target}" == "validator" ]]; then
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
  if [[ "${target}" == "validator" ]]; then
    # A readonly, offline build must hit the SDK seed without compiling it.
    # A missing cache COPY or stale warm flags fails the image smoke gate.
    docker run --rm --network none --read-only --tmpfs /tmp --user 65532:65532 \
      "${image}:${tag}" sh -ec '
        test "$GOCACHEPROG" = /usr/local/bin/sdo-go-cache
        cd /opt/sdo/sdo/operator/sdk
        go list -x -export ./program >/tmp/sdk-export 2>/tmp/sdk-build
        go vet -x ./program 2>>/tmp/sdk-build
        if grep -qE "/(compile|vet) " /tmp/sdk-build; then
          cat /tmp/sdk-build >&2
          echo "validator SDK cache is missing or stale" >&2
          exit 1
        fi
      '
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
