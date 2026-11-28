Incident toy-incident in namespace toy: investigate and fix it. There may be several root causes that mask each other or arrive later. Reason from first principles: test each hypothesis; correlate failing user tests with the components on their path. You have kubectl, the shell and the `sdo` command (skills at /opt/sdo/skills). After a change, wait for its effect, not a fixed time. On detector alerts, done is SDO's recovered verdict; it ends your work. Never print a credential value.
Opened on Service/component-0 unavailable at 22:13:20 UTC.
Learned playbooks for this incident (read the README; reuse its scripts where they fit):
- repair-0: its detector is firing; .sdo/playbooks/repair-0/README.md, diagnose.go
CANDIDATES (on every failing path unless marked k of n, plus one hop upstream)
api  on every failing path
  -1m Deployment/api changed vs accepted source (spec.template) by kubectl
live: sdo detect (this view); sdo changes and sdo timeline list every row
Answer in the `incident_result` field and leave `reflection` null.
Additional environment-specific instructions:
Task telemetry (mode open): no source is bound; omit telemetry detectors. Bindings are read-only.
Use the toy incident transport.
