Incident toy-incident in namespace toy.
Opened on Service/component-0 unavailable at 22:13:20 UTC. Recovered waits for these alerts and any user test that starts failing early in the incident to clear; `sdo detect` and `sdo verify` show the current set.
Read your skills: /opt/sdo/skills/incident-response/SKILL.md and /opt/sdo/skills/sdo-operating/SKILL.md.
Start with `sdo detect`. Test competing hypotheses with parallel sub-agents (see incident-response).
CANDIDATES (on every failing path unless marked k of n, plus one hop upstream)
api  on every failing path
  -1m Deployment/api changed vs accepted source (spec.template) by kubectl
live: sdo detect (this view); sdo changes and sdo timeline list every row
Answer in the `incident_result` field and leave `reflection` null.

Additional environment-specific instructions:
Task telemetry (mode open): no source is bound; omit telemetry detectors. Bindings are read-only.
Use the toy incident transport.
