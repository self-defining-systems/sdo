#!/usr/bin/env bash
# Write a kubeconfig that can do everything inside one namespace and nothing outside it,
# for an operator that runs one app copy (lane). Idempotent.
#
#   deploy/lane-kubeconfig.sh CONTEXT NAMESPACE OUT [DURATION]   # DURATION default 168h
#
# Namespace-scoped: a ServiceAccount fuzzops-operator, bound in the namespace (RoleBinding) to
# the built-in "admin" ClusterRole. Nothing cluster-scoped is created.
set -euo pipefail
CTX=${1:?usage: lane-kubeconfig.sh CONTEXT NAMESPACE OUT [DURATION]}
NS=${2:?}
OUT=${3:?}
DUR=${4:-168h}
k() { kubectl --context "$CTX" -n "$NS" "$@"; }
k get sa fuzzops-operator >/dev/null 2>&1 || k create sa fuzzops-operator >/dev/null
k get rolebinding fuzzops-operator >/dev/null 2>&1 ||
  k create rolebinding fuzzops-operator --clusterrole=admin --serviceaccount="$NS:fuzzops-operator" >/dev/null
TOKEN=$(k create token fuzzops-operator --duration="$DUR")
SERVER=$(kubectl config view --raw -o jsonpath="{.clusters[?(@.name==\"$CTX\")].cluster.server}")
CA=$(kubectl config view --raw -o jsonpath="{.clusters[?(@.name==\"$CTX\")].cluster.certificate-authority-data}")
umask 077
cat >"$OUT" <<YAML
apiVersion: v1
kind: Config
clusters:
- name: $CTX
  cluster: {server: "$SERVER", certificate-authority-data: "$CA"}
users:
- name: fuzzops-operator@$NS
  user: {token: "$TOKEN"}
contexts:
- name: $CTX-$NS
  context: {cluster: $CTX, user: fuzzops-operator@$NS, namespace: $NS}
current-context: $CTX-$NS
YAML
echo "$OUT"
