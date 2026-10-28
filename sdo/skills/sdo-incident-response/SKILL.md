---
name: sdo-incident-response
description: Fix incidents
---

Accepted source and developer changes express intent. Operational memory is fallible advice. Recovery requires evidence from the intended application behavior. Tools: [CLI](references/commands.md). Use only help-confirmed options; on a usage error, read help, don't guess. [Auth](references/database-credentials.md). [CA](references/webhook-ca.md).

1. Read `playbook_coverage` and [matched repair guidance](references/coverage.md); check current evidence and apply useful matches. README guides judgement.
2. Report one cause per independently fixable fault, plus late faults; each its own action, applied together; combine only faults that can't be fixed separately; continue until it verifies; findings may regress.
3. Use recorded actions for mutations; declare every changed resource and never hand-write receipts. Use one wrapper, client-go reads and commit source repairs. Follow [live repair guidance](references/live-repairs.md) for drift and blocked admission.
4. SDO's independent verification is the source of truth; wait on it instead of sleeping. `healthy` is not done while unexplained differences from accepted source remain: restore each or justify it. SDO resumes once within its bound. Inspect rollout blockers and old-template Pods before repeating writes. Cite live cause observations, explained_detectors, verification; never claim done without this self-check. If findings self-clear, return cancelled rather than inventing one.
5. Controller snapshots govern, even empty; flag possible regressions; cap stdout. A complete JSON report: evidence/errors/schema. Verified warning is informational; reads/reports stay; closed-incident mutations are rejected.
6. Undo proven harms; keep useful repairs. Clean up temporary investigation resources; label helper Pods/Jobs `sdo.dev/responder-helper=true`, never application workloads.
7. Fix `still_broken`; inspect `verification_blockers`; keep `already_done`/`prior_response_progress`; controller rechecks. Merge peer learning.

Read the playbook index and its one-line descriptions; open relevant READMEs. Large context and help link to full session files; extract needed fields.

Source before raw commands.

Unfinished: blockers.

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
