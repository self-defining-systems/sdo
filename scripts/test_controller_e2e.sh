#!/usr/bin/env bash
# End-to-end controller test with no cluster, images, network or model:
# the real sdo-operator controller (with its memory-host subprocess, broker and
# accepted-memory git repository) against a real kube-apiserver and etcd from
# envtest, a fake kubelet, and the no-model agent scripts through the real
# worker and tool transport. See docs/testing-guide.md#controller-end-to-end.
#
#   bash scripts/test_controller_e2e.sh [scenario1|scenario2|api-errors|hung-agent|all]
#
# Needs: bubblewrap with user namespaces, git, kubectl, Go, uv, and the envtest
# binaries (KUBEBUILDER_ASSETS, else scripts/install_envtest.sh installs them).
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
scenario="${1:-all}"
case "$scenario" in
    scenario1) selection='^TestFirstIncidentLearnsThenRepeatIsDeterministic$' ;;
    scenario2) selection='^TestControllerSurvivesFaults$' ;;
    api-errors) selection='^TestControllerToleratesAPIErrors$' ;;
    hung-agent) selection='^TestControllerEndsHungAgent$' ;;
    all) selection='^(TestFirstIncidentLearnsThenRepeatIsDeterministic|TestControllerSurvivesFaults|TestControllerToleratesAPIErrors|TestControllerEndsHungAgent)$' ;;
    *) echo "usage: $0 [scenario1|scenario2|api-errors|hung-agent|all]" >&2; exit 2 ;;
esac
for tool in bwrap git kubectl go uv; do
    command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 2; }
done
bwrap --ro-bind / / --dev /dev --proc /proc true 2>/dev/null || {
    echo "bubblewrap cannot create user namespaces here (kernel.apparmor_restrict_unprivileged_userns=0 on Ubuntu 24.04)" >&2
    exit 2
}
if [[ -z "${KUBEBUILDER_ASSETS:-}" ]]; then
    KUBEBUILDER_ASSETS="$(bash "$root/scripts/install_envtest.sh")"
    export KUBEBUILDER_ASSETS
fi
export SDO_E2E_CACHE="${SDO_E2E_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/sdo-e2e}"
cd "$root"
uv sync --extra test -q
cd sdo/operator/runtime
# The cluster, controller and agents are real processes: run in real time with a generous bound.
exec go test -tags 'integration controller_e2e' ./e2e -run "$selection" -count=1 -timeout 25m -v
