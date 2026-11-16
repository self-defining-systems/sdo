Incident toy-incident in namespace toy.
Opened on Service/component-0 unavailable at 22:13:20 UTC.
Read your skills: /opt/sdo/skills/incident-response/SKILL.md and /opt/sdo/skills/sdo-operating/SKILL.md.
Start with `sdo detect`. Work the incident yourself. You may start read-only subagents to test explanations in parallel when that saves time; run them in the foreground, because ending your turn submits your result. You alone change things. Return only to give up.
CANDIDATES (on every failing path unless marked k of n, plus one hop upstream)
api  on every failing path
  -1m Deployment/api changed vs accepted source (spec.template) by kubectl
live: sdo detect (this view); sdo changes and sdo timeline list every row
Answer in the `incident_result` field and leave `reflection` null.

Additional environment-specific instructions:
Task telemetry (mode open): no source is bound; omit telemetry detectors. Bindings are read-only.
Use the toy incident transport.
