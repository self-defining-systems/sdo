#!/bin/bash
# Print the last termination state and previous logs of a pod: restart-reason.sh <namespace> <pod>
set -euo pipefail
namespace="$1"
pod="$2"
kubectl -n "$namespace" get pod "$pod" -o jsonpath='{range .status.containerStatuses[*]}{.name}{" "}{.lastState.terminated.reason}{" "}{.lastState.terminated.exitCode}{"\n"}{end}'
kubectl -n "$namespace" logs "$pod" --previous --all-containers --tail=50
