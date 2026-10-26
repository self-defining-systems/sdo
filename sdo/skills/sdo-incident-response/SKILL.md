---
name: sdo-incident-response
description: Resolve incidents.
---

# Incident response

Source and developer changes express intent; memory is fallible advice. Recovery needs evidence from intended behavior. See [commands](references/commands.md). Use jq. [Credentials](references/database-credentials.md).

1. Read `playbook_coverage`: matches/bindings, can_mitigate, unexplained findings. Success/failure counts inform, never gate permission. A diagnosis-only match supplies evidence, not repair. Check status and late findings; check live preconditions and accepted-source values. Investigate contradictions.
2. Apply fixes or independent mitigations together; continue until SDO verifies. Preserve partial repairs; new findings may be regressions.
3. Record live mutations and declare every changed resource; never hand-write receipts. API reads follow client-go. Commit source repairs. Restore namespace drift to accepted source unless live evidence proves an intended difference; uncertainty alone does not block restoration. Explain benign drift with live evidence. Menu IDs persist; recheck live conditions. If admission blocks recovery with a missing backend, restore it if possible; otherwise scoped fail-open or removal to restore availability is acceptable; report it. Test/report playbook edits. Learn scripts with budgeted relation-derived Values.
4. SDO's independent verification is authoritative; wait on it instead of sleeping. Inspect rollout blockers and old-template Pods before repeating writes. Action success is not recovery. Pending traffic is unproven; reasons name scenarios. Verify fresh observations. Report repaired causes promptly, including late faults. Cite live cause observations, explained_detectors and verification results; never claim done without this self-check. If findings clear without repair, return cancelled rather than inventing one.
5. Controller progress, even empty snapshots, is authoritative; summaries flag possible regressions within stdout limits. Complete JSON holds evidence/errors and schema. Incident-verified warning is informational; reads/reports remain; closed-incident mutations are rejected. Grace and reflection continue in maintenance.
6. Undo proven harmful changes; retain useful repairs. Clean up temporary investigation resources; label helper Pods or Jobs `sdo.dev/responder-helper=true`, never application workloads.
7. Fix `still_broken`; keep `already_done`. The controller independently checks the result. Merge accepted peer learning on memory conflicts.

Read typed action failure stages; fix source inputs before raw commands.

Prioritize current user-facing failures; defer harmless cleanup. Report unfinished work only with a blocker or exhausted options.

Refs: printed JSON or Kind/[namespace/]name.

Use relative paths; scratch outside memory. Read full output files. Finish blocking operations in one call instead of polling across model turns. Static files belong in static_context, not live cause evidence.
