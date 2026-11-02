#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tag="${SDO_IMAGE_TAG:-v0.1.0}"
# Provenance must describe the bytes Bake consumes, including untracked sources.
source_changes="$(git -C "${repo_root}" status --porcelain --untracked-files=normal -- scripts/build_sdo_images.sh scripts/sdo-images.hcl)"
if [[ -n "${source_changes}" ]]; then
  echo "image source has uncommitted changes; commit before building source-bound images" >&2
  exit 1
fi
export SDO_SOURCE_REVISION="$(git -C "${repo_root}" rev-parse HEAD)"
# Source-checked images also carry the identity of exactly the files their build
# context sends (image_provenance.py), so commits outside those files reuse them.
provenance="${repo_root}/benchmarks/sregym/fastloop/image_provenance.py"
for target in controller; do
  built_changes="$(cd "${repo_root}" && python3 "${provenance}" dirty "${target}" HEAD)"
  if [[ -n "${built_changes}" ]]; then
    echo "image source has uncommitted changes; commit before building source-bound images: ${built_changes}" >&2
    exit 1
  fi
done
export SDO_CONTROLLER_SOURCE_TREE="$(cd "${repo_root}" && python3 "${provenance}" identity controller HEAD)"
# CI builds only changed targets through Bake, then uses these same smoke gates.
read -r -a selected <<<"${SDO_IMAGES:-controller responder benchmark script-tools}"
for image in "${selected[@]}"; do
  case "${image}" in
  controller | responder | benchmark | script-tools) ;;
  *)
    echo "unknown SDO image target: ${image}" >&2
    exit 1
    ;;
  esac
done
selected_image() { [[ " ${selected[*]} " == *" $1 "* ]]; }
if [[ "${SDO_SKIP_IMAGE_BUILD:-0}" != "1" ]]; then
  export SDO_IMAGE_TAG="${tag}"
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
      test "$GOFLAGS" = "-mod=mod -p=2 -trimpath"
      cd /opt/sdo/sdo/operator/sdk
      go list ./program >/dev/null
    '
    docker run --rm --user 65532:65532 \
      --volume "${repo_root}/tests/fixtures/sdo/playbook_program_app:/fixture:ro" \
      "${image}:${tag}" sdo-operator validation-child /fixture /opt/sdo/sdo/operator ""
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
      python3 -c "from benchmarks.sregym.fastloop.generated_worker import main" >/dev/null
  fi
fi

# Live image qualification is an explicit opt-in because it spends provider
# quota. Release builds enable it; ordinary CI never reads account credentials.
if [[ "${SDO_CLAUDE_SMOKE:-0}" == "1" ]]; then
  smoke_image="sdo-responder:${tag}"
  if selected_image benchmark && [[ "${SDO_SCRIPTED_WORKER:-0}" != "1" ]]; then
    smoke_image="sdo-sregym-responder:${tag}"
  elif ! selected_image responder; then
    echo "Claude smoke requires a model-backed responder target" >&2
    exit 1
  fi
  test -f "${SDO_CLAUDE_CREDENTIALS_FILE:?set the Claude credentials file for the explicit live smoke}"
  docker run --rm --read-only --tmpfs /tmp:rw,mode=1777 --user "$(id -u):$(id -g)" \
    --security-opt no-new-privileges --cap-drop ALL \
    --mount "type=bind,src=${SDO_CLAUDE_CREDENTIALS_FILE},dst=/sdo/credentials/.credentials.json,readonly" \
    --mount "type=bind,src=${SDO_OPUS_POOL_DIR:?set the existing host provider pool},dst=/sdo/opus-pool" \
    --env CLAUDE_CONFIG_DIR=/tmp/.claude --env SDO_OPUS_POOL_DIR=/sdo/opus-pool \
    --env SDO_OPUS_MAX_SESSIONS --env SDO_OPUS_POOL_MODE --env SDO_OPUS_POOL_HOST_ID \
    "${smoke_image}" python3 /usr/local/lib/sdo/check_claude_worker.py
fi
