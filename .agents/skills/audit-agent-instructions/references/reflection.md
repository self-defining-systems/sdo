# Reflection workflow

Source: `sdo/agents/responder/reflection_worker.py`, `reflection.py`,
`reflection_authoring.py`, `reflection_brief.py` and shared memory validation.
The task selects resume/fresh mode; do not infer the mode from the role name.

1. Read the independently verified closure/outcome evidence after every closed
   incident, including failures; resume the responder session when requested and
   available, otherwise use the explicitly supplied compact fresh-session brief.
2. Compare each verified mechanism with accepted playbooks and detectors. Refine
   fitting reusable logic; fix or retire mitigations that failed or were undone.
3. Use the authoring skills to create diagnosis, mitigation, README and detector
   coverage, with role bindings and real recorded-observation replay evidence.
   Missing captures alone do not block evidence already recorded in the session.
4. Edit only authorized final files in the proposal checkout, preserve accepted
   corpus/provenance and update the index. A README alone is not executable learning.
5. Run the shared memory check, correct diagnostics and return the same typed
   decision and logical IDs; report concrete typed SDK gaps for unsupported causes.

Check per-cause evidence and rejected-edit feedback, not just a generic success
sentence. Keep resumed-session prompts limited to new outcome data. The broker
independently validates and publishes; reflection never sees grader verdicts.
