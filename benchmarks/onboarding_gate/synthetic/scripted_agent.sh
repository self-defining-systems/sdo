# Scripted onboarding of the synthetic notes app: what a good agent does, without a model.
# Each "# step NAME" block is one Bash tool call (scripted_claude.py). Runs in /work.
# step scaffold
sdo-operator onboarding init --repo /work --namespace "$GATE_NAMESPACE" --entry notes:8080
# step write-probes
set -e
cp /kit/fixture/generators.go .sdo/diagnostics/traffic/generators/generators.go
mkdir -p .sdo/diagnostics/traffic/workloads .sdo/playbooks/health-objective
cp /kit/fixture/workloads/*.yaml .sdo/diagnostics/traffic/workloads/
cp /kit/fixture/health-objective.md .sdo/playbooks/health-objective/README.md
python3 - <<'PY'
import re
path = ".sdo/arch.md"
text = open(path).read()
begin, end = text.index("<!-- sdo:application-map:begin -->"), text.index("<!-- sdo:application-map:end -->")
region = text[begin:end]
start, close = region.index("\noperations:"), region.rindex("```")
region = region[:start + 1] + open("/kit/fixture/map.yaml").read() + region[close:]
open(path, "w").write(text[:begin] + region + text[end:])
PY
# step plan
sdo-operator onboarding journeys --repo /work --namespace "$GATE_NAMESPACE" --plan
# step quick
sdo-operator onboarding journeys --repo /work --namespace "$GATE_NAMESPACE" --quick --only write-note
# step journeys
sdo-operator onboarding journeys --repo /work --namespace "$GATE_NAMESPACE"
# step commit
git add -A .sdo && git commit -qm "Onboard the synthetic notes app" && git status --short
