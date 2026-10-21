#!/usr/bin/env bash
# Prepare an immutable kind image cache; no Kubernetes cluster is created.
set -euo pipefail

if [[ $# -ne 4 ]]; then
    echo "usage: $0 OUTPUT_NODE_IMAGE CONTROLLER_IMAGE RESPONDER_IMAGE VALIDATOR_IMAGE" >&2
    exit 2
fi
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_image="$1"
shift
base_image="${TOY_BASE_NODE_IMAGE:-kindest/node:v1.34.0}"
container=sdo-toy-preload
export GOMAXPROCS=2
export GOFLAGS="${GOFLAGS:-} -p=2"
export CGO_ENABLED=0
if docker container inspect "$container" >/dev/null 2>&1; then
    echo "refusing to replace existing container $container" >&2
    exit 1
fi
scratch="$(mktemp -d)"
created=0
cleanup() {
    if [[ "$created" == 1 ]]; then docker rm -f "$container" >/dev/null; fi
    rm -rf "$scratch"
}
trap cleanup EXIT
started=$SECONDS
go build -buildvcs=false -trimpath -o "$scratch/toy" "$root/apps/toy/main.go"
cp "$root/apps/toy/Dockerfile" "$scratch/Dockerfile"
docker build --network=none --provenance=false -t sdo-toy-app:v1 "$scratch"
mapfile -t cni_images < <(cd "$root" && uv run python -c 'from benchmarks.sregym.fastloop.toy_cni import CALICO_IMAGES; print("\n".join(CALICO_IMAGES))')
docker image inspect "$base_image" "$@" "${cni_images[@]}" >/dev/null
# A single archive deduplicates shared runtime/compiler layers.
image_platform="$(go env GOOS)/$(go env GOARCH)"
docker save --platform "$image_platform" -o "$scratch/images.tar" sdo-toy-app:v1 "$@" "${cni_images[@]}"
created=1
docker run --name "$container" --privileged --pull=never \
    --volume "$scratch/images.tar:/images.tar:ro" \
    --entrypoint /bin/sh "$base_image" -ec '
        containerd >/tmp/toy-containerd.log 2>&1 &
        daemon=$!
        trap "kill $daemon 2>/dev/null || true" EXIT
        ready=0
        for attempt in $(seq 1 100); do
            if ctr version >/dev/null 2>&1; then ready=1; break; fi
            sleep 0.1
        done
        test "$ready" = 1 || { cat /tmp/toy-containerd.log; exit 1; }
        ctr --namespace k8s.io images import --no-unpack --label io.cri-containerd.image=managed /images.tar
        kill "$daemon"
        wait "$daemon" || true
        trap - EXIT
        rm -f /tmp/toy-containerd.log
        sync
    '
base_entrypoint="$(docker image inspect --format '{{json .Config.Entrypoint}}' "$base_image")"
base_command="$(docker image inspect --format '{{json .Config.Cmd}}' "$base_image")"
if [[ "$base_command" == null ]]; then base_command='[]'; fi
docker commit --change "ENTRYPOINT $base_entrypoint" --change "CMD $base_command" \
    --change 'LABEL sdo.dev/toy-image-cache=true' "$container" "$output_image" >/dev/null
printf 'Prepared %s in %ss. Use fastloop up --app toy --toy-node-image %s\n' "$output_image" "$((SECONDS-started))" "$output_image"
