#!/usr/bin/env bash
# Build the scripted-responder test images from SDO images.
#
#   bash benchmarks/sregym/assurance/build_images.sh [BASE_TAG] [TAG]
#
# BASE_TAG (default v0.1.0) names the SDO images to wrap; TAG (default assure)
# names the results. The base images are pinned under an `<image>:<TAG>-base`
# tag first, so a later rebuild of the shared tags cannot change a run midway.
# BASE_TAG=checkout builds the bases from this checkout instead, without
# touching the shared tags. The validator runs no model and is only re-tagged.
set -euo pipefail

base_tag="${1:-v0.1.0}"
tag="${2:-assure}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

if [ "${base_tag}" = "checkout" ]; then
  docker tag "sdo-detector-validator:v0.1.0" "sdo-detector-validator:${tag}"
  for target in controller responder; do
    docker build --quiet --file "${repo_root}/sdo/operator/Dockerfile.runtime" --target "${target}" \
      --tag "sdo-${target}:${tag}-base" "${repo_root}"
  done
  docker build --quiet --file "${repo_root}/benchmarks/sregym/Dockerfile.worker" --target sregym-responder \
    --build-arg "SDO_RESPONDER_IMAGE=sdo-responder:${tag}-base" \
    --tag "sdo-sregym-responder:${tag}-base" "${repo_root}"
else
  docker tag "sdo-detector-validator:${base_tag}" "sdo-detector-validator:${tag}"
  for image in sdo-controller sdo-sregym-responder; do
    docker tag "${image}:${base_tag}" "${image}:${tag}-base"
  done
fi
for image in sdo-controller sdo-sregym-responder; do
  docker build --quiet \
    --file "${repo_root}/benchmarks/sregym/assurance/Dockerfile.scripted" \
    --build-arg "BASE=${image}:${tag}-base" \
    --tag "${image}:${tag}" \
    "${repo_root}"
done
# The scripted CLI answers agentshim's probe, and no real model CLI is left.
docker run --rm --user 65532:65532 "sdo-sregym-responder:${tag}" codex --help >/dev/null
docker run --rm --user 65532:65532 "sdo-controller:${tag}" sh -c '! command -v claude && codex --version'
