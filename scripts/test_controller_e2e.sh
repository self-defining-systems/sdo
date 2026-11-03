#!/usr/bin/env bash
# End-to-end controller test with no cluster, images, network or model:
# the real sdo-operator controller (with its memory-host subprocess, broker and
# accepted-memory git repository) against a real kube-apiserver and etcd from
# envtest, a fake kubelet, and the no-model agent scripts through the real
# worker and tool transport. See docs/testing-guide.md#controller-end-to-end.
#
#   bash scripts/test_controller_e2e.sh [scenario1|scenario2|all|api-errors|hung-agent]
#
# api-errors and hung-agent reproduce two open controller findings (they fail today) and are not part of `all`.
#
# Needs: bubblewrap with user namespaces, git, kubectl, Go, uv, and the envtest
# binaries (KUBEBUILDER_ASSETS, else `setup-envtest use` is tried).
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
scenario="${1:-all}"
case "$scenario" in
    scenario1) selection='^TestFirstIncidentLearnsThenRepeatIsDeterministic$' ;;
    scenario2) selection='^TestControllerSurvivesFaults$' ;;
    all) selection='^(TestFirstIncidentLearnsThenRepeatIsDeterministic|TestControllerSurvivesFaults)$' ;;
    api-errors) selection='^TestControllerToleratesAPIErrors$'; export SDO_E2E_API_ERRORS=1 ;;
    hung-agent) selection='^TestControllerEndsHungAgent$'; export SDO_E2E_HUNG_AGENT=1 ;;
    *) echo "usage: $0 [scenario1|scenario2|all|api-errors|hung-agent]" >&2; exit 2 ;;
esac
for tool in bwrap git kubectl go uv; do
    command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 2; }
done
bwrap --ro-bind / / --dev /dev --proc /proc true 2>/dev/null || {
    echo "bubblewrap cannot create user namespaces here (kernel.apparmor_restrict_unprivileged_userns=0 on Ubuntu 24.04)" >&2
    exit 2
}
if [[ -z "${KUBEBUILDER_ASSETS:-}" ]]; then
    if command -v setup-envtest >/dev/null; then
        KUBEBUILDER_ASSETS="$(setup-envtest use 1.34.x -p path)"
        export KUBEBUILDER_ASSETS
    else
        echo "set KUBEBUILDER_ASSETS to a directory with etcd and kube-apiserver (go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest)" >&2
        exit 2
    fi
fi
export SDO_E2E_CACHE="${SDO_E2E_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/sdo-e2e}"
cd "$root"
uv sync --extra test -q
cd sdo/operator
# The cluster, controller and agents are real processes: run in real time with a generous bound.
exec go test -tags 'integration controller_e2e' ./e2e -run "$selection" -count=1 -timeout 25m -v
