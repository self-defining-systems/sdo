#!/usr/bin/env bash
set -euo pipefail

# A deterministic end-to-end smoke of the production Go host, lifecycle,
# broker, validator, installer, and long-running controller. The fixture agents
# use typed task/result contracts and make no model calls.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cluster_name="${SDO_RUNTIME_SMOKE_CLUSTER:-sdo-runtime-smoke}"
image_tag="${SDO_IMAGE_TAG:-v0.1.0}"
workspace="$(mktemp -d)"
application="${workspace}/application"
control_namespace="demo-sdo"
buildkit_image="${SDO_SMOKE_BUILDKIT_IMAGE:-moby/buildkit@sha256:ef31818fffd0b9133d710742e06b492553f7ebc54f5fd66cd21a1a9bf2e51f76}"

cleanup() {
  local status="$?"
  if [[ "${status}" != "0" ]]; then
    kubectl --namespace "${control_namespace}" get pods,jobs,configmaps 2>/dev/null || true
    kubectl --namespace "${control_namespace}" logs job/sdo-controller-run --tail=100 2>/dev/null || true
  fi
  if [[ "${SDO_SMOKE_KEEP_CLUSTER:-0}" != "1" ]]; then
    kind delete cluster --name "${cluster_name}" >/dev/null 2>&1 || true
  fi
  rm -rf "${workspace}"
  return "${status}"
}
trap cleanup EXIT

cat >"${workspace}/kind.yaml" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: ${cluster_name}
networking:
  disableDefaultCNI: true
  podSubnet: 192.168.0.0/16
nodes:
  - role: control-plane
EOF
kind create cluster --config "${workspace}/kind.yaml"

# The validator's positive and deny canaries require enforced NetworkPolicy.
calico_url="https://raw.githubusercontent.com/projectcalico/calico/v3.32.1/manifests/calico.yaml"
calico_sha256="a1df919d9721cf667accdc3e72848911b0cb25cfab7d2478ad0c996302c95744"
curl --fail --location --silent --show-error --retry 3 "${calico_url}" --output "${workspace}/calico.yaml"
echo "${calico_sha256}  ${workspace}/calico.yaml" | shasum -a 256 --check
kubectl create -f "${workspace}/calico.yaml"
kubectl --namespace kube-system rollout status daemonset/calico-node --timeout=300s
kubectl --namespace kube-system rollout status deployment/calico-kube-controllers --timeout=300s

docker build --load \
  --file "${repo_root}/tests/fixtures/sdo/native_smoke/Dockerfile" \
  --build-arg "SDO_IMAGE_TAG=${image_tag}" \
  --tag sdo-worker:native-smoke "${repo_root}"
kind load docker-image \
  "sdo-controller:${image_tag}" \
  "sdo-responder:${image_tag}" \
  "sdo-detector-validator:${image_tag}" \
  sdo-worker:native-smoke --name "${cluster_name}"

kubectl create namespace "${control_namespace}"
# Fixture workers do not use model credentials or image builds. The distinct
# Secrets still exercise the production credential and registry mount policy.
kubectl --namespace "${control_namespace}" create secret generic sdo-agent-credentials \
  --from-literal=SDO_SMOKE_DUMMY=unused
echo '{"auths":{}}' >"${workspace}/dockerconfig.json"
kubectl --namespace "${control_namespace}" create secret generic sdo-registry-push \
  --type=kubernetes.io/dockerconfigjson \
  --from-file=".dockerconfigjson=${workspace}/dockerconfig.json"

mkdir -p "${application}"
git -C "${application}" init -q
git -C "${application}" config user.name "SDO Native Smoke"
git -C "${application}" config user.email "sdo-native-smoke@invalid"
printf '%s\n' 'Native SDO smoke application' >"${application}/README.md"
git -C "${application}" add README.md
git -C "${application}" commit -qm "seed native smoke application"

(cd "${repo_root}/sdo/operator" && go build -o "${workspace}/sdo-operator" ./cmd/sdo-operator)
timeout 1200s "${workspace}/sdo-operator" host \
  --repository "${application}" \
  --namespace demo \
  --control-namespace "${control_namespace}" \
  --application demo \
  --health-objective 'The demo Deployment has an available replica.' \
  --operation-id native-smoke \
  --max-attempts 1 \
  --controller-image "sdo-controller:${image_tag}" \
  --responder-image "sdo-responder:${image_tag}" \
  --validator-image "sdo-detector-validator:${image_tag}" \
  --buildkit-image "${buildkit_image}" \
  --repository-pvc sdo-application-repository \
  --credentials-secret sdo-agent-credentials \
  --registry-push-secret sdo-registry-push \
  --model fixture \
  --agent-registrations "${repo_root}/tests/fixtures/sdo/native_smoke/registrations" \
  --agent-registration-configmap sdo-native-smoke-agents \
  >"${workspace}/receipt.json"
jq -e '.installed == true and .goal_commit != "" and .schema_commit != "" and .source_commit != "" and .architecture_commit != "" and .judge_commit != ""' \
  "${workspace}/receipt.json"
kubectl --namespace demo wait --for=condition=available deployment/demo --timeout=180s

deadline=$((SECONDS + 300))
while (( SECONDS < deadline )); do
  state="$(kubectl --namespace "${control_namespace}" get configmap sdo-controller-state \
    --output jsonpath='{.data.runtime-state\.json}' 2>/dev/null || true)"
  if jq -e '
    [.history[] | select(.detector_id == "health-objective")] | last | .status == "clear"
  ' <<<"${state}" >/dev/null 2>&1 && jq -e '
    [.history[] | select(.detector_id == "service-endpoints")] | last | .status == "clear"
  ' <<<"${state}" >/dev/null 2>&1; then
    echo 'Native host, lifecycle, deployment, validation, and controller smoke passed.'
    exit 0
  fi
  sleep 3
done
echo 'Native controller never published clear health detector evaluations.' >&2
exit 1
