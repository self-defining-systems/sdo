---
name: sdo-incident-response
description: Fix incidents
---

Accepted source and developer changes express intent. Operational memory is fallible advice. Recovery requires evidence from the intended application behavior. [CLI](references/commands.md). Use help-confirmed options. [Auth](references/database-credentials.md). [CA](references/webhook-ca.md). [Investigate](../sdo-incident-navigation/SKILL.md).

`external_message`: follow [external response](references/external-incidents.md); self-check, return completed.

1. Start from checked stage receipts, residual findings; keep settled repairs. Clear blockers first; re-trigger stalled saved repairs, re-check saved diagnoses. Use `playbook_coverage` and [matches](references/coverage.md); [apply fixes](references/fixes.md).
2. Report one cause per independently fixable fault, and late faults. Start independent repairs together; combine edits to one object, keeping each cause's evidence separate. [Causes](references/causes.md).
3. Use recorded actions for mutations; declare every changed resource and never hand-write receipts. Use one wrapper, client-go reads; source repairs: [patches](../sdo-patching/SKILL.md). Follow [repairs](references/live-repairs.md) for drift and blocked admission.
4. Wait for independent verification, traffic SLOs too. On `verification_status=awaiting_responder_completion`, finish with evidence; otherwise wait until verified, unless out-of-reach: report it, finish. Restore unexplained source differences or justify them; SDO may resume. Inspect rollout blockers and old-template Pods before repeating writes. Cite live cause observations, explained_detectors and verification; never claim done without this self-check. If findings self-clear, return cancelled rather than inventing one.
5. Honor controller snapshots, even empty; flag regressions. Cap stdout; report evidence/errors/schema. Once SDO verifies, mutations are rejected; closure does not wait for final narration.
6. Undo proven harms; keep useful repairs. Clean up temporary investigation resources; label helper Pods/Jobs `sdo.dev/responder-helper=true`, never application workloads.
7. Fix `still_broken`; inspect `verification_blockers`; keep `already_done`/`prior_response_progress`; controller rechecks. Merge peer learning; reflect on new evidence, detection gaps.
Report memory-check timeouts; no retry.

Scratch outside memory; finish in one call, no polling. Static files belong in static_context, not live cause evidence.

<!-- command-topics -->
## Command topics

- [act](references/commands/act.md)
- [check](references/commands/check.md)
- [detect](references/commands/detect.md)
- [diagnose](references/commands/diagnose.md)
- [menu](references/commands/menu.md)
- [mitigate](references/commands/mitigate.md)
- [report](references/commands/report.md)
- [rollback](references/commands/rollback.md)
- [source](references/commands/source.md)
- [status](references/commands/status.md)
- [verify](references/commands/verify.md)
