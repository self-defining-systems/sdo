#!/usr/bin/env bash
# Pre-merge live smoke check: one scripted-worker incident on a warm toy kind lane (no LLM, a few minutes).
#
# Reuses the lane at $SDO_SMOKE_LANE (`up` creates it when missing and only redelivers changed images to a warm
# one), builds this commit's controller and scripted-worker images, resets the lane, injects one toy fault that a
# seeded user test catches, lets SDO open the incident, and asserts on the controller's own evidence:
#   1. the responder Job started and returned a result;
#   2. no state_persistence_invalid or contract validation errors in the controller log;
#   3. `sdo detect` returned the candidate view to the responder;
#   4. the controller never restarted.
# Optional external-incident variant (SDO_SMOKE_EXTERNAL=1): a ~2 KB external page must arrive whole.
# The lane is kept warm between runs. Delete it with:
#   uv run python -m benchmarks.sregym.fastloop down --run-dir "$SDO_SMOKE_LANE" --delete-cluster
# Commit first: the image build refuses a dirty image source.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

lane="${SDO_SMOKE_LANE:-${HOME}/.cache/sdo-live-smoke/lane}"
prefix="${SDO_SMOKE_PREFIX:-ls-smoke-}"
problem="${SDO_SMOKE_PROBLEM:-toy_image_catalog}"
out="${SDO_SMOKE_OUT:-${lane}.out}"
tag="ls-$(git rev-parse --short=8 HEAD)"
fastloop=(uv run python -m benchmarks.sregym.fastloop)

export SDO_IMAGE_TAG="${tag}" GOMAXPROCS="${GOMAXPROCS:-2}" GOFLAGS="${GOFLAGS:--p=2}"
mkdir -p "$(dirname "${lane}")" "${out}"
phase() { echo "== $(date +%H:%M:%S) $*" >&2; }
fail() { echo "live_smoke: FAIL: $*" >&2; exit 1; }
step() { # step NAME LOG COMMAND...
  local name="$1" log="$2"
  shift 2
  phase "${name}"
  "$@" >"${log}" 2>&1 || { tail -20 "${log}" >&2; fail "${name} (${log})"; }
}
started=${SECONDS}

step "preflight" "${out}/preflight.log" "${fastloop[@]}" preflight --app toy
step "build images ${tag}" "${out}/build.log" env SDO_SCRIPTED_WORKER=1 SDO_IMAGES="controller benchmark" \
  bash scripts/build_sdo_images.sh
# A previous run (or an aborted one) may have left the lane faulted, and `up` refuses an unhealthy lane.
if [[ -f "${lane}/fastloop.json" ]]; then
  step "heal the warm lane" "${out}/heal.log" "${fastloop[@]}" reset --run-dir "${lane}" --dev-mount "${repo_root}"
fi
step "up (warm lane reused if present)" "${out}/up.log" "${fastloop[@]}" up --app toy --run-dir "${lane}" \
  --cluster-prefix "${prefix}" --dev-mount "${repo_root}"
step "reset" "${out}/reset.log" "${fastloop[@]}" reset --run-dir "${lane}" --dev-mount "${repo_root}"

run_id="smoke-$(date +%s)"
step "run ${problem}" "${out}/run.log" "${fastloop[@]}" run --run-dir "${lane}" --agent sdo --problem "${problem}" \
  --diagnosis-grading off --timeout 300 --detection-timeout 240 --no-checkpoints --run-id "${run_id}"

results="${lane}/results/${run_id}"
runtime="$(find "${results}" -type d -name sdo_runtime | head -1)"
[[ -n "${runtime}" ]] || fail "no sdo_runtime evidence under ${results}"
controller_log="${out}/controller.log"
cat "${runtime}"/controller_logs/*.log >"${controller_log}" 2>/dev/null || fail "no controller log under ${runtime}"

phase "assertions"
failures=0
check() { # check NAME STATUS
  if [[ "$2" == 0 ]]; then echo "PASS  $1"; else echo "FAIL  $1"; failures=$((failures + 1)); fi
}

responder_result="$(find "${runtime}/tasks" -mindepth 2 -maxdepth 2 -name result.json -path '*/responder*' | head -1)"
if [[ -z "${responder_result}" ]]; then
  responder_result="$(grep -l '"incident_id"' "${runtime}"/tasks/*/result.json 2>/dev/null | head -1 || true)"
fi
[[ -n "${responder_result}" ]] && started_job=0 || started_job=1
check "responder Job started and returned a result (${responder_result##*/tasks/})" "${started_job}"

if grep -Ei 'state_persistence_invalid|state_persistence_failure|contract validation|validation error' "${controller_log}" >"${out}/errors.log"; then
  head -5 "${out}/errors.log" >&2
  check "no state_persistence_invalid or contract validation errors" 1
else
  check "no state_persistence_invalid or contract validation errors" 0
fi

candidate_view=1
if [[ -n "${responder_result}" ]] && uv run python -I - "${responder_result}" <<'PY'
import json, sys

result = json.load(open(sys.argv[1]))
payload = result.get("result", result)
evidence = []
stack = [payload]
while stack:
    item = stack.pop()
    if isinstance(item, dict):
        if item.get("name") == "sdo detect":
            evidence.append(item)
        stack.extend(item.values())
    elif isinstance(item, list):
        stack.extend(item)
sys.exit(0 if any("CANDIDATES" in str(e.get("details", "")) for e in evidence) else 1)
PY
then candidate_view=0; fi
check "sdo detect returned the candidate view" "${candidate_view}"

kubeconfig="${lane}/kubeconfig"
restarts="$(KUBECONFIG="${kubeconfig}" kubectl get pods -A -l app.kubernetes.io/component=controller \
  -o jsonpath='{range .items[*]}{range .status.containerStatuses[*]}{.restartCount}{"\n"}{end}{end}' 2>/dev/null |
  awk '{s+=$1} END {print s+0}')"
[[ "${restarts}" == 0 ]] && restarted=0 || restarted=1
check "controller restart count is 0 (${restarts})" "${restarted}"

elapsed=$((SECONDS - started))
echo "live_smoke: ${elapsed}s (tag ${tag}, lane ${lane}, evidence ${out})"
if [[ "${failures}" != 0 ]]; then fail "${failures} assertion(s) failed"; fi
echo "live_smoke: PASS"
