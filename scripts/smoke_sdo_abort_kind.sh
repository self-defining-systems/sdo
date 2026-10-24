#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${SDO_ABORT_SMOKE_CLUSTER:?set the disposable kind lane explicitly}"
: "${SDO_ABORT_SMOKE_RUN_DIR:?set the evidence directory explicitly}"
: "${SDO_ABORT_SMOKE_LOCK:?set the shared lane lock file explicitly}"
mkdir -p "${SDO_ABORT_SMOKE_RUN_DIR}"
exec 9>"${SDO_ABORT_SMOKE_LOCK}"
flock --nonblock 9 || { echo 'lane lock is held; no cluster changes made' >&2; exit 1; }
cd "${repo_root}"
mkdir -p .fastloop
(cd sdo/operator && CGO_ENABLED=0 go build -buildvcs=false -o ../../.fastloop/sdo-operator ./cmd/sdo-operator)
if ! kind get nodes --name "${SDO_ABORT_SMOKE_CLUSTER}" >/dev/null 2>&1; then
  kind create cluster --name "${SDO_ABORT_SMOKE_CLUSTER}"
fi
kind get kubeconfig --name "${SDO_ABORT_SMOKE_CLUSTER}" >"${SDO_ABORT_SMOKE_RUN_DIR}/kubeconfig"
base_image="${SDO_ABORT_SMOKE_BASE_IMAGE:-sdo-controller:l4}"
worker_image="${SDO_ABORT_SMOKE_WORKER_IMAGE:-sdo-sregym-responder:l4-scripted}"
controller_image="sdo-controller:abort-smoke"
build_dir="${SDO_ABORT_SMOKE_RUN_DIR}/image-context"
mkdir -p "${build_dir}/.fastloop" "${build_dir}/sdo"
cp .fastloop/sdo-operator "${build_dir}/.fastloop/"
cp -a sdo/operator "${build_dir}/sdo/"
docker build --network=none --file "${repo_root}/tests/fixtures/sdo/abort_smoke/Dockerfile" \
  --build-arg "BASE_IMAGE=${base_image}" --tag "${controller_image}" "${build_dir}"
uv run python -m benchmarks.sregym.fastloop.image_delivery --cluster "${SDO_ABORT_SMOKE_CLUSTER}" --run-dir "${SDO_ABORT_SMOKE_RUN_DIR}" \
  "${controller_image}" "${worker_image}" sdo-toy-app:v1
SDO_OPERATOR_BIN="${repo_root}/.fastloop/sdo-operator" \
KUBECONFIG="${SDO_ABORT_SMOKE_RUN_DIR}/kubeconfig" \
uv run python tests/fixtures/sdo/abort_smoke/run.py \
  --run-dir "${SDO_ABORT_SMOKE_RUN_DIR}" --controller-image "${controller_image}" --worker-image "${worker_image}"
