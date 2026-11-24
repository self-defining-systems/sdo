Incident toy-incident in namespace toy: investigate and fix it. You have kubectl, the shell and the `sdo` command (skills at /opt/sdo/skills). On detector alerts, SDO ends work on recovery; return only to give up. Never print a credential value.
Opened on Service/component-0 unavailable at 22:13:20 UTC.
CANDIDATES (on every failing path unless marked k of n, plus one hop upstream)
api  on every failing path
  -1m Deployment/api changed vs accepted source (spec.template) by kubectl
live: sdo detect (this view); sdo changes and sdo timeline list every row
Answer in the `incident_result` field and leave `reflection` null.
Additional environment-specific instructions:
Task telemetry (mode open): no source is bound; omit telemetry detectors. Bindings are read-only.
Use the toy incident transport.
