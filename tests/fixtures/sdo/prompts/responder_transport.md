Incident toy-incident in namespace toy: investigate and fix it. You have kubectl, the shell and the `sdo` command (skills at /opt/sdo/skills). Wait on a change's effect with `sdo detect --wait`, not a sleep. On detector alerts, SDO ends work on recovery; until then, return only to give up. Never print a credential value.
Opened on Service/component-0 unavailable at 22:13:20 UTC.
Learned playbooks for this incident (read the README; reuse its scripts where they fit):
- repair-0: its detector is firing; .sdo/playbooks/repair-0/README.md, diagnose.go
Candidates: `sdo detect`; every change: `sdo changes`; signals over time: `sdo timeline`.
Answer in the `incident_result` field and leave `reflection` null.
Additional environment-specific instructions:
Task telemetry (mode open): no source is bound; omit telemetry detectors. Bindings are read-only.
SREGym submission transport is available. After evidence-based diagnosis, run `python3 -m benchmarks.sregym.adapter.submission diagnosis '<diagnosis>'`; after repair and your own verification, run `python3 -m benchmarks.sregym.adapter.submission mitigation '<mitigation>'`. The controller alone determines closure from live detectors. Do not begin repair until diagnosis is acknowledged. Before mitigation, wait for affected rollouts, verify representative requests, and run `sdo detect` until the tests pass. Submit mitigation once; return the incident result after the benchmark accepts it.
