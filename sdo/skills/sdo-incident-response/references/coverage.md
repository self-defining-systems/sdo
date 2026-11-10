# Coverage and learned repairs

`playbook_coverage`: matches/bindings, mitigation, unexplained findings. Counts inform; never permission gates. Diagnosis-only matches supply evidence. Apply `can_mitigate` matches by running their `mitigate_command`; check rollout, current status, late findings, preconditions and accepted-source values. Investigate contradictions. Keep verified stage receipts; `plan_not_executable` is terminal for that revision and binding, while other bindings remain eligible; `ineffective` means the playbook already applied here and left those findings open. Partial playbooks: stages/`sdo mitigate NAME` handle mechanics; README guides judgement.

Ready matches include named object/scalar bindings and the command that applies them. Use that supplied
evidence to act on useful independent matches together; spend further investigation on contradictions or
missing preconditions. Stage actions have already happened; begin with the remaining work.

An applied source repair with unavailable readback remains pending and unverified; inspect its application
before repeating the write. A nonmatching or broken learned program is evidence to investigate the live cause
and use a direct recorded repair. Avoid spending the incident rewriting a generic program; reflection saves
the verified repair afterward.

The first message lists open findings only (`fingerprint: summary`; the rest counted in `findings_omitted`), coverage matches that explain them, and `stage_actions`: one line per automatic repair with the objects it wrote and what it left: `cleared`; `reverted-ineffective` (it did not help, so SDO restored those objects and the original fault is in place); `kept-partial` (it fixed part; keep it); `kept-irreversible` (a delete, exec or restart stays). Stage-cleared findings are done; start from what remains. Full facts, including `prior_stages` receipts and finding evidence, are in `details_file`.

`checked_stage_receipts` names each exact application and whether its scoped verification completed
successfully without rollback, error or pending rollout. It is not whole-incident closure. Join those IDs
to `prior_stages[].mitigations[].apply_receipt` in the full facts file when more evidence is needed;
`findings` in the brief is the residual work. Do not reapply settled work or probe known report shapes.
A fully repaired incident may finish deterministically without starting a responder.

Recovery is controller-owned. `verify` and closure consume the same fresh
post-repair health observations and stability proof. Completed program receipts
prove their own scope; remaining unhealthy behavior goes to the responder with
those receipts. A repaired workload's old children require live disappearance or confirmed
lifecycle equivalence, not another repair or a claim that they are benign. Final narration does not
hold outcome publication after recovery and worker quiescence.

For staged multi-fault work, clear blockers that prevent other repairs (admission, quota, scheduling,
security policy) first, then re-run or re-trigger stalled saved repairs instead of re-deriving them.
Start independent repairs together (`sdo mitigate A --bind ... -- B --bind ...`); combine repairs editing one object into one edit. Re-check with
saved diagnoses before closing. The brief is enough to start; no mandatory skill reread.

`stage_actions` retains controller result labels and adds `blockers` from the saved rollout conditions
and warning events, beside the affected program. `applied_cleared` is shown as `cleared`;
`rollout_pending_at_handoff` means applied but rollout pending; failed and skipped results remain explicit.
`skipped (already_attempted)` means another program planned the same write that an earlier stage action had
already applied and that was still settling; SDO did not repeat it, and recovery belongs to that earlier action.
`skipped (holds_data)` means the program would have restarted, deleted, rewritten or scaled down a workload the
application map marks `holds_data`, so SDO did not run it. The line names the action; the object is unchanged and the
finding is yours. The restart may still be right, but look at what holds the data and what it holds first.
The same skip covers a command, script, Job, HTTP call or database-user change that runs in, binds or names a
`holds_data` component: SDO cannot see what it does to the stored records.
Blocker messages are observations at handoff, so confirm whether they still apply. Absence of a blocker
message is not proof that a rollout can progress. Full unabridged facts stay in `details_file`.
`stage_playbooks` maps involved program IDs to accepted README files (null if unavailable), including
finding-linked playbooks without a current match, settled programs and matches whose findings have since cleared. Other playbooks remain optional context.

The automatic stage observes saved writes together while dependent health and traffic settle.
It waits while its writes roll out and then for the rolling window of each detector whose finding
depends on them, within its recovery deadline. A repair outside Kubernetes objects (a container command,
HTTP call, database user, Job or script) is proved when its own result succeeded and its program's
diagnosis stops matching; the findings it targeted wait on it like on a write. It hands off for uncovered work, verified blockers,
failures, findings that outlived their window, or its recovery deadline: an `applied_still_matching`
action you receive has had that window, so do not just wait for it again.
Clearance alone does not close the incident: it needs fresh traffic samples after the write, and no
applied program still diagnosing the fault. Otherwise you receive the findings that cleared without
that proof. Read the program's diagnose evidence and live traffic, not only detector states.
The same decision is rechecked before a first worker claim; prepared requests alone
do not force a model call after recovery. Existing worker ownership is rejoined.
`prior_stages[].recovery_wait` records typed `exit_reason` and measured `duration`;
its application receipts retain the same observation. A deadline is unconfirmed,
not failed execution. Successful waits and already-satisfied ensures count as
completed actions without adding writes. Use the residual findings to choose work;
dependency admission grants observation time, not new explanation or repair credit.
