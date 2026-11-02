# Fastloop report fields

Each row comes from `results/<timestamp>-sdo/incidents.jsonl`. The outcome documents are in `<run-dir>/application_workspace/.sdo/outcomes/*.json`. The [analyze-experiment references](../../analyze-experiment/SKILL.md) define every field.

## Per incident

| Report column | Source |
| --- | --- |
| Image delivery and fallback reason | row `image_delivery`, `image_delivery_reason`; also recorded in the lane fastloop.json and image_delivery.jsonl |
| Fault, oracle result, diagnosis grade | row `problem_id`, `oracle`, `diagnosis_grade` |
| Caught within the detection timeout | row `incident_id`, `detected_at`, `error`, checked against the opening finding in the outcome; a detection miss counts as uncaught even when recovery later passes |
| Detection path (health, investigator, program sweep) | detector ID of the opening finding: `drift/investigator`, `drift/<program>`, or a health detector |
| Injection to detection; TTM | `injection_to_detection_seconds`; `detection_to_mitigation_seconds` |
| Resolution path | `resolution_path`, `playbook_coverage.matches[].playbook_id`, `playbook_coverage.matches[].explained_finding_fingerprints`, `playbook_coverage.unexplained_finding_fingerprints`; actual repairs in `plan_applications` |
| Tokens | `responder_tokens`, `drift_tokens`, `reflection_tokens` (input, cached, output), and `reflection_seconds` |
| Learning | the controller ledger's `reflection_decision` (`learning_decision`, `error`); the programs committed under `.sdo/playbooks/` |
| Program matches | the outcome's `program_runs[].verdict.result` and `program_runs[].binding_source` (`finding` or `hint`), plus the row's match fields |
| `sdo` tool use | `tool_commands` (`plan`, `apply`, `verify`, `diagnose`) |
| Drift verdicts | the outcome's `drift_events[].investigation_verdict` and `drift_events[].investigation_reason` |

## Summary

- Score against the baseline of the same stream, and against the previous SDO run.
- Mean TTM and mean responder input. Split them before and after the responder first applied each learned mitigation or a sweep caught a fault.
- For a latent stream, report caught / injected by fault class, keeping health, investigator and program catches separate. A null detection time is unknown, not zero; an incident with a confirmed opening finding still counts as caught when its timing is missing. An unrelated health finding in a composite does not prove the latent component was detected.
- Count repeats on new targets caught without the investigator: the opening finding must name a learned `drift/<program>`, with no investigator dispatch for that fault in the controller log. Confirm from the accepted commits that the program was learned after the class's first catch; use the finding's drift-event source to distinguish a periodic sweep from classification of a change. Report this count over all repeats, including misses, separately from agent-selected learned mitigation (coverage plus applied and verified program evidence).
- For each failure: the root cause with `file:line`, the evidence directory, and the cheapest test that reproduces it.
- State plainly what the run does not show. For example, a scripted run does not show learning.
