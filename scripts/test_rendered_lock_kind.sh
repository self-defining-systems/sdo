#!/usr/bin/env bash
# LK1 scripted Level 6. Never selects or mutates another lane.
# Caller must hold the shared lane-53 flock for this entire command.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${SDO_LK1_RUN_DIR:?set an evidence directory outside the repository}"
case "$(realpath -m "$SDO_LK1_RUN_DIR")" in
    "$root"|"$root"/*) echo "LK1 evidence directory must be outside the repository" >&2; exit 1 ;;
esac
mkdir -p "$SDO_LK1_RUN_DIR"
export SDO_LK1_KUBECONFIG="$SDO_LK1_RUN_DIR/kubeconfig"
if kind get clusters | rg -qx 'luna-pilot-53'; then
    kind export kubeconfig --name luna-pilot-53 --kubeconfig "$SDO_LK1_KUBECONFIG"
else
    kind create cluster --name luna-pilot-53 --image kindest/node:v1.34.0 --kubeconfig "$SDO_LK1_KUBECONFIG" --wait 90s
fi
# Some cached Docker archives retain a multi-platform index without every
# platform blob. Import only the host platform, into this lane's single node.
platform="$(docker image inspect busybox:1.36 --format '{{.Os}}/{{.Architecture}}')"
docker save busybox:1.36 | docker exec -i luna-pilot-53-control-plane \
    ctr --namespace=k8s.io images import --platform "$platform" --digests --snapshotter=overlayfs -
cd "$root/sdo/operator/runtime"
go test -run '^TestRenderedLockKindTwoIncidents$' -v -count=1 -timeout=4m .
