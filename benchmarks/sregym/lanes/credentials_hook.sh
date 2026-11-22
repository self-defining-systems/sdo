#!/bin/sh
# The credentials hook of a real-agent lane (`fastloop run --credentials-hook`): fastloop calls it with the SDO control
# namespace after every row-start wipe, KUBECONFIG set to the lane's. It puts the agent's credential file into the
# `sdo-agent-credentials` Secret BY REFERENCE (`--from-file`): the value never passes through this script's output.
#   SREGYM_LANES_AUTH_FILE  the credential file on this host (the spec's operator.settings.auth_file)
#   SREGYM_LANES_AUTH_KEY   the Secret key the agent pod reads (.credentials.json for claude, auth.json for codex)
set -eu
ns=${1:?usage: credentials_hook.sh CONTROL_NAMESPACE}
file=${SREGYM_LANES_AUTH_FILE:-}
key=${SREGYM_LANES_AUTH_KEY:-}
if [ -z "$file" ] || [ ! -f "$file" ]; then
  echo "credentials hook: SREGYM_LANES_AUTH_FILE must name an existing credential file (got '${file}')" >&2
  exit 1
fi
if [ -z "$key" ]; then
  echo "credentials hook: SREGYM_LANES_AUTH_KEY is not set" >&2
  exit 1
fi
kubectl get ns "$ns" >/dev/null 2>&1 || kubectl create ns "$ns" >/dev/null
kubectl -n "$ns" create secret generic sdo-agent-credentials "--from-file=$key=$file" --dry-run=client -o yaml |
  kubectl -n "$ns" apply -f - >/dev/null
