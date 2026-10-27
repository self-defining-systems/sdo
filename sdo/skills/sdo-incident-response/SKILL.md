---
name: sdo-incident-response
description: Fix incidents
---

Accepted source and developer changes express intent. Operational memory is fallible advice. Recovery requires evidence from the intended application behavior. Discover capabilities and argument shapes: [CLI](references/commands.md). Use only help-confirmed options; on a usage error, read help, don't guess. Use jq. [Auth](references/database-credentials.md).

1. `playbook_coverage`: matches/bindings, mitigation, unexplained findings. Success/failure counts inform; never permission gates. A diagnosis-only match supplies evidence, not a repair; apply a `can_mitigate` match first, by playbook ID; check while it rolls out. Check current status and any late findings; quick live check of their preconditions and accepted-source values. Investigate contradictions. Skip verified prior_stages.
2. Report one cause per independently fixable fault, plus late faults; each its own action, applied together; combine only faults that can't be fixed separately; continue until it verifies; new findings may regress.
3. Use recorded actions for live mutations (declare every changed resource); never hand-write receipts. One call/wrapper. API reads follow client-go. Commit source repairs. Restore namespace drift to accepted source unless live evidence establishes an intended difference; uncertainty alone doesn't block it. Explain benign drift with live evidence. When an admission control blocks recovery and its backend is gone, restore the backend if you can. Otherwise a scoped fail-open or removal that restores availability is acceptable; report it. Repairs stay on the app's own objects; never widen a selector or policy to other namespaces.
4. SDO's independent verification is the source of truth; wait on it instead of sleeping. `healthy` is not done while unexplained differences from accepted source remain: restore each or state why it's intended before returning. Inspect rollout blockers and old-template Pods before repeating writes. Verify fresh state. Cite live cause observations, explained_detectors, verification; never claim done without this self-check. If findings self-clear, return cancelled rather than inventing one.
5. Controller snapshots govern, even empty; flag possible regressions; bound stdout. A complete JSON report: evidence/errors/schema. Verified warning is informational; reads/reports stay; closed-incident mutations are rejected.
6. Undo proven harms; keep useful repairs. Clean up temporary investigation resources; label helper Pods/Jobs `sdo.dev/responder-helper=true`, never application workloads.
7. Fix `still_broken`; keep `already_done`; controller rechecks. Merge peer learning on conflicts.

Fix source inputs before raw commands.

Unfinished: blocker or exhausted options.

Refs: JSON or Kind/[namespace/]name.

Relative paths; non-memory scratch. Calls finish in one call instead of polling across model turns. Static files belong in static_context, not live cause evidence.
