Incident toy-incident in namespace toy.
Opened on Service/component-0 unavailable at 22:13:20 UTC.
Read your skills: /opt/sdo/skills/incident-response/SKILL.md and /opt/sdo/skills/sdo-operating/SKILL.md.
Start with `sdo detect`. After `sdo detect`, name your lead explanation and the runner-up. Unless a failing response already states the cause, start a read-only sub-agent for the runner-up before you act on the lead (see incident-response). Check the failing operation itself, and return only to give up.
Candidates: `sdo detect`; every change: `sdo changes`; signals over time: `sdo timeline`.
Answer in the `incident_result` field and leave `reflection` null.

Additional environment-specific instructions:
Task telemetry (mode open): no source is bound; omit telemetry detectors. Bindings are read-only.
