# Fastloop report fields

Each row comes from `results/<timestamp>-sdo/incidents.jsonl`. The outcome documents are in `<run-dir>/application_workspace/.sdo/outcomes/*.json`. The [analyze-experiment references](../../analyze-experiment/SKILL.md) define every field.

## Per incident

| Report column | Source |
| --- | --- |
| Fault, oracle result, diagnosis grade | row `problem_id`, `oracle`, `diagnosis_grade` |
| Detection path (health, investigator, program sweep) | detector ID of the opening finding: `drift/investigator`, `drift/<program>`, or a health detector |
| Injection to detection; TTM | `injection_to_detection_seconds`; `detection_to_mitigation_seconds` |
| Resolution path | `resolution_path`, `fast_path_programs`, `fast_path_skipped_reason`, `fast_path_relation_covered_findings` |
| Tokens | `responder_tokens`, `drift_tokens`, `reflection_tokens` (input, cached, output), and `reflection_seconds` |
| Learning | the controller ledger's `reflection_decision` (`learning_decision`, `error`); the programs committed under `.sdo/playbooks/` |
| Program matches | the outcome's `program_runs[].verdict.result` and `program_runs[].binding_source` (`finding` or `hint`), plus the row's match fields |
| `sdo` tool use | `tool_commands` (`plan`, `apply`, `verify`, `diagnose`) |
| Drift verdicts | the outcome's `drift_events[].investigation_verdict` and `drift_events[].investigation_reason` |

## Summary

- Score against the baseline of the same stream, and against the previous SDO run.
- Mean TTM and mean responder input. Split them before and after the point where each program first took the fast path or a sweep caught a fault.
- For each failure: the root cause with `file:line`, the evidence directory, and the cheapest test that reproduces it.
- State plainly what the run does not show. For example, a scripted run does not show learning.
