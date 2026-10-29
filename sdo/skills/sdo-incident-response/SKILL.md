---
name: sdo-incident-response
description: Fix incidents
---

Accepted source and developer changes express intent. Operational memory is fallible advice. Recovery requires evidence from the intended application behavior. [CLI](references/commands.md). Use only help-confirmed options; on a usage error, read help, don't guess. [Auth](references/database-credentials.md). [CA](references/webhook-ca.md).

1. Start from checked stage receipts and residual findings; keep settled repairs. Clear blockers first; re-trigger stalled saved repairs, then re-check saved diagnoses. Use `playbook_coverage` and [matched repair guidance](references/coverage.md).
2. Report one cause per independently fixable fault, plus late faults. Start independent repairs together; combine edits to one object, keeping each cause's evidence separate.
3. Use recorded actions for mutations; declare every changed resource and never hand-write receipts. Use one wrapper, client-go reads and commit source repairs. Follow [live repair guidance](references/live-repairs.md) for drift and blocked admission.
4. SDO's independent verification is the source of truth; wait on it instead of sleeping. On `verification_status=awaiting_responder_completion`, finish with evidence; otherwise wait until verified. Restore unexplained source differences or justify them; SDO may resume. Inspect rollout blockers and old-template Pods before repeating writes. Cite live cause observations, explained_detectors and verification; never claim done without this self-check. If findings self-clear, return cancelled rather than inventing one.
5. Controller snapshots govern, even empty; flag possible regressions; cap stdout. A complete JSON report: evidence/errors/schema. Once SDO verifies, mutations are rejected; closure does not wait for final narration.
6. Undo proven harms; keep useful repairs. Clean up temporary investigation resources; label helper Pods/Jobs `sdo.dev/responder-helper=true`, never application workloads.
7. Fix `still_broken`; inspect `verification_blockers`; keep `already_done`/`prior_response_progress`; controller rechecks. Merge peer learning; reflect only on new evidence.

Checked schemas/receipts avoid discovery; files retain evidence.

Source before raw commands.

Scratch outside memory; finish in one call instead of polling across model turns. Static files belong in static_context, not live cause evidence.

<!-- command-topics -->
## Command topics

- [act](references/commands/act.md)
- [apply](references/commands/apply.md)
- [check](references/commands/check.md)
- [detect](references/commands/detect.md)
- [diagnose](references/commands/diagnose.md)
- [menu](references/commands/menu.md)
- [mitigate](references/commands/mitigate.md)
- [plan](references/commands/plan.md)
- [report](references/commands/report.md)
- [rollback](references/commands/rollback.md)
- [source](references/commands/source.md)
- [status](references/commands/status.md)
- [verify](references/commands/verify.md)
