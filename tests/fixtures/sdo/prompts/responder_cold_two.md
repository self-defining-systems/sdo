Incident toy-incident in namespace toy: investigate and fix it. There may be several independent root causes; faults can mask each other or arrive later. Reason from first principles: form hypotheses, test each, correlate failing user journeys to the components on their path. You have kubectl, the shell and the `sdo` command (skills at /opt/sdo/skills). After a change, wait for its effect, not a fixed time. On detector alerts, SDO ends work on recovery; until then, return only to give up. Never print a credential value.
Opened on Service/component-0 unavailable at 22:13:20 UTC.
Candidates: `sdo detect`; every change: `sdo changes`; signals over time: `sdo timeline`.
Answer in the `incident_result` field and leave `reflection` null.
Additional environment-specific instructions:
Task telemetry (mode open): no source is bound; omit telemetry detectors. Bindings are read-only.
