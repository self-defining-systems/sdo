Incident toy-incident in namespace toy.
Opened on Service/component-0 unavailable at 22:13:20 UTC.
Read your skills: /opt/sdo/skills/incident-response/SKILL.md and /opt/sdo/skills/sdo-operating/SKILL.md.
Start with `sdo detect`. Work the incident yourself. You may start read-only subagents to test explanations in parallel when that saves time; run them in the foreground, because ending your turn submits your result. You alone change things. Return only to give up.
Candidates: `sdo detect`; every change: `sdo changes`; signals over time: `sdo timeline`.
Answer in the `incident_result` field and leave `reflection` null.

Additional environment-specific instructions:
Task telemetry (mode open): no source is bound; omit telemetry detectors. Bindings are read-only.
SREGym submission transport is available. After evidence-based diagnosis, run `python3 -m benchmarks.sregym.adapter.submission diagnosis '<diagnosis>'`; after repair and your own verification, run `python3 -m benchmarks.sregym.adapter.submission mitigation '<mitigation>'`. The controller alone determines closure from live detectors. Do not begin repair until diagnosis is acknowledged. Before mitigation, wait for affected rollouts, verify representative requests, and run `sdo detect` until the tests pass. Submit mitigation once; return the incident result after the benchmark accepts it.
