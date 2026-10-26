# Native reflection and retirement evidence

### Partial repair reflection

Aggregate `classification=failed` does not erase independently verified repairs.
For an existing executable repair, the broker admits a confirmed cause only when every cause resource has a successful,
timed repair receipt linked to its controller application (or exact-equivalent manual repair): not dry-run or
rolled back, no application/rollback error, affirmative verification and final exact
equivalence, and revision/binding-scoped post-apply NoMatch in its matching tool
transcript. A successful command, flat NoMatch, or worker-authored verification
claim alone is insufficient. This includes controller-verified equivalent manual
repairs. Missing independent proof remains unlearned. Overlapping cause groups on one object are ambiguous under resource-level receipts and conservatively abstain; an unverified application cannot inherit another application’s proof on the same object. No benchmark grade is used.

Cold first-generation repairs need no prior program or plan. Their trusted recorded
mutation receipts must follow dispatch and the original resource-bound triggering
findings. The latest error-free controller `verify` must evaluate those detectors
after the mutation, with no remaining finding on any cause resource (including
related refs). A peer may remain active, or a complete controller all-clear may
prove the repair even though the authoritative aggregate closure remains failed. A later write invalidates older
clearance; missing triggers, stale evaluation, or same-resource cause ambiguity
abstain. Read-only operations and worker-authored success claims do not qualify.

Controller verification records above the existing 2KiB opaque-response cap retain
a typed `verification_summary`: version, completeness, exact open-finding count,
all distinct primary/related open-resource refs, and detector IDs/status/timestamps.
The controller derives it before dropping verbose findings. If even this summary
cannot fit, it is omitted; learning abstains instead of using counts or mutable
response files. Historical truncated calls without this summary remain unknown.
The worker sees only repaired refs, supporting evaluations/time, and the count of
other open findings, without the peer cause or opaque transcript.

The committed outcome retains all causes, receipts, and its failed classification.
Its `reflection_eligibility.causes` contains only verified repaired cause groups,
with indices into the full outcome. Unverified causes are represented separately
as typed `unassigned` questions with `reason=evidence_gap`; they do not authorize
executable learning. The worker
receives compact cause indices and only those causes, verified receipts and typed
applications/diagnoses and scoped cold-verification evidence. Opaque tool transcripts and unattributed playbook/drift
claims are omitted from that learning view. No global recovery, positive fixed
capture, program trust credit, or repaired peer is inferred from partial repair.
A missing fixed capture remains an evidence gap. Full-success reflection retains
its existing behavior.

Every closed incident with a resumable responder session requires reflection,
including failed, cancelled and self-healed incidents with no verified repair.
The worker resumes that session by default, independently of the response budget
and benchmark cleanup. Settled corpus work does not bypass this dispatch.
Without a verified repaired cause, reflection may return `no_change` with a
concrete evidence-gap reason or typed SDK gaps; it cannot publish an `updated`
README or executable claim. A responder Job failure preserves its provider checkpoint from the incident tool
state even when no worker result was delivered. When no resumable session exists,
the accepted outcome carries `reflection_skip_reason=no_session`; reflection
never falls back to a fresh session. The ledger's skip decision also names
`no_session`.

Shared `memory check` and broker publication validate every changed detector,
program, script and library against the committed incident's verified cause
bindings. Positive corpus bindings or controller-settled equivalent repairs bind
playbooks; detector references and shared-library imports must preserve that
binding. A typed gap for cause A does not authorize executable changes for
unverified cause B; gaps for either cause remain allowed.
Reflection execution uses its frozen task deadline for both the wait context and
Kubernetes Job `activeDeadlineSeconds`. A Pending worker is bounded too. Expiry
records a terminal `ReflectionDecision` with `learning_decision=failed` and
`failure_reason=timeout`, then cleans the proposal; it is not a validation retry.
Restart retains the original task/deadline and cannot create another Job once
that deadline expires. Leadership cancellation before expiry remains retryable.
Completed result recovery precedes execution: after Job TTL deletion, the frozen
request/result identities must agree and no owned nonterminal Pod may remain.
The result's server creation time (or the live Job's completion time) must fall
within the original deadline; recovering that result after expiry does not grant
a new execution budget. An unresolved prior result never authorizes a new Job.

The persisted broker ledger, task ID and task payload preserve one logical
reflection across restarts; validation corrections are separate attempts.
When reflection runs, its usage follows the existing decision/result ledger path,
including across retries. A pre-cleanup benchmark grade and cleanup status remain
separate measurements; neither authorizes production learning.

## RL1 learning content and isolation

Verified full and partial causes require executable diagnosis and mitigation, a handling README, and detector coverage. Go reports reflection.program-required or reflection.detector-required and retries incomplete results; README-only updated is a failed attempt. The only exception is a typed SDK gap naming the cause resources and reason. Controller capture gaps are telemetry, not a learning gate: fixtures may be authored from recorded actions and existing session observations. Publication compiles programs and executes mitigation to prove Match becomes NoMatch. Capture digest matching and rename/peer generalization are no longer save-time prerequisites. Broker-validated new programs receive one origin trust credit from their verified cause/bound fixture. Benchmark grading may finish while reflection is running. Grading evidence stays outside production inputs and memory; reflection receives only the original SDO outcome, and cleanup still waits for its drain. No grader or oracle verdict supplies production verification.


### Expired incident retirement between encounters

As a restart backstop, an expired response request with no active Job or orphan
worker Pod is archived through the failed-closure outbox with typed reason
`CLOSURE_FAILURE_REASON_AGENT_TIMEOUT`. API uncertainty preserves the binding;
a live worker may still be rejoined. The archive is persisted before another
incident's effects can execute. This terminal archive has no health success or
new learning credit. New observations can open a fresh incident even when the
old fault is still firing. Already committed closures retain their receipts.
Stale-health evidence retirement precedes expiry retirement and preserves its
original visible failure and single notification. Expiry skips incidents already
claimed by a pending or terminal closure; a failed closure from an earlier
incident does not claim the current incident.

Learning-origin Git reads use the durable accepted repository, not a retired
proposal checkout. Missing optional origin evidence logs Git stderr and earns
no extra credit, while responder dispatch remains available.

## Closure grading evidence

The conductor-backed adapter observes the correlated native ledger while lifecycle
and strict-receipt collection continue. A closure with a responder result owns one
submission attempt, independent of reflection success. `sdo_incident_grade.json`
(schema `sdo.sregym-incident-grade/v1`) stores the frozen `closure`, `incident_id`,
`submission_started_at`, `status`, raw submission responses in `grade`,
`graded_at`, `closure_to_grade_seconds` (from closure `verified_at`), and
`responder_runtime_seconds` (`dispatched_at` to `responder_completed_at`). Missing
timestamps leave durations absent. `graded_at` is observed grading completion.
The conductor API exposes acknowledgments and terminal status, not oracle verdicts;
join actual diagnosis/mitigation success from the harness result CSV by incident/run.
A grading callback that returns an original-worker verdict preserves that verdict.

The adapter persists its submission claim before network I/O. `submitting` after
restart or `failed` with an error is ambiguous/failed transport, never success;
it is not automatically retried. Reflection cannot replace this file or initiate
another submission. The terminal strict receipt and accepted outcome remain
separate evidence; neither depends on a benchmark grade. A closure without a
responder result sends nothing. Grade files live outside the operational repository
and are never inputs to responders or reflection.
