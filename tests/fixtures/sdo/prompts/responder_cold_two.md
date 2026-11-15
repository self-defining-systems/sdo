Incident toy-incident in namespace toy.
Opened on Service/component-0 unavailable at 22:13:20 UTC.
Read your skills: /opt/sdo/skills/incident-response/SKILL.md and /opt/sdo/skills/sdo-operating/SKILL.md.
Start with `sdo detect`. Name every plausible explanation for what users see. Start one subagent per explanation (in the background) to test it read-only with evidence, and brief each to check whether the data and state on its component match what the failing operation needs, not only whether it is up. You are the orchestrator: do not investigate yourself, weigh the subagents' evidence, and make the fix only when the evidence points to one cause. Combine facts across reports, not only verdicts. Return only to give up.
Candidates: `sdo detect`; every change: `sdo changes`; signals over time: `sdo timeline`.
Answer in the `incident_result` field and leave `reflection` null.

Additional environment-specific instructions:
Task telemetry (mode open): no source is bound; omit telemetry detectors. Bindings are read-only.
