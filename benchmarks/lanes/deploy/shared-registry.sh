#!/usr/bin/env bash
# The image registry of a shared host cluster (provider vcluster), reachable by every node without root.
#
#   deploy/shared-registry.sh up          # create it in the host cluster (idempotent) and wait for it
#   deploy/shared-registry.sh port        # print the NodePort (the registry is at localhost:PORT on every node)
#
# It is a Deployment with a NodePort Service in namespace sdo-shared-infra. containerd treats a `localhost:PORT`
# registry as insecure (plain http) with no node configuration, and kube-proxy answers NodePorts on localhost, so a
# pod that names its image `localhost:30500/<path>` pulls it on any node, and `docker push localhost:30500/<path>`
# works from a host of the cluster. There is no transparent mirror (that needs /etc/rancher/k3s/registries.yaml on
# every node, i.e. root): images of lanes are named with deploy/registry.sh ref.
# Settings: VCLUSTER_HOST_KUBECONFIG (required), FUZZOPS_SHARED_REGISTRY_PORT (30500), FUZZOPS_SHARED_REGISTRY_SIZE (60Gi).
set -euo pipefail
cd "$(dirname "$0")"
. ./vcluster-lib.sh
PORT=${FUZZOPS_SHARED_REGISTRY_PORT:-30500}
SIZE=${FUZZOPS_SHARED_REGISTRY_SIZE:-60Gi}
NS=sdo-shared-infra

case "${1:-}" in
up)
  vc_host_kubeconfig
  hk apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Namespace
metadata: {name: $NS}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: registry, namespace: $NS}
spec:
  accessModes: [ReadWriteOnce]
  resources: {requests: {storage: $SIZE}}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: registry, namespace: $NS}
spec:
  replicas: 1
  strategy: {type: Recreate}
  selector: {matchLabels: {app: registry}}
  template:
    metadata: {labels: {app: registry}}
    spec:
      containers:
      - name: registry
        image: registry:2
        env: [{name: REGISTRY_STORAGE_DELETE_ENABLED, value: "true"}]
        ports: [{containerPort: 5000}]
        volumeMounts: [{name: data, mountPath: /var/lib/registry}]
        resources: {requests: {cpu: 50m, memory: 128Mi}}
      volumes: [{name: data, persistentVolumeClaim: {claimName: registry}}]
---
apiVersion: v1
kind: Service
metadata: {name: registry, namespace: $NS}
spec:
  type: NodePort
  selector: {app: registry}
  ports: [{port: 5000, targetPort: 5000, nodePort: $PORT}]
YAML
  hk -n "$NS" rollout status deploy/registry --timeout=180s >/dev/null
  echo "shared registry at localhost:$PORT on every node"
  ;;
port) echo "$PORT" ;;
*) echo "usage: shared-registry.sh up | port" >&2; exit 2 ;;
esac
