# Native reflection and retirement evidence

### Partial repair reflection

Aggregate `classification=failed` does not erase independently verified repairs (this section is the failed-closure rule; `success` outcomes use the LEARN3 rule below).
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
as typed `unassigned` questions with `reason=no_recorded_repair`; they do not authorize
executable learning. Since LEARN3 (2026-10-27), on a `success` outcome (closure
verified healthy) a cause is verified by a recorded successful repair action
that wrote one of its resources or an object they control. Per-cause finding
clearance, exclusive resource ownership, a pre-incident capture and a separate
post-action observation are not required: the repeat's act-verify loop checks the
saved steps. When no pre-incident healthy capture exists, the controller binds the
closure's own `fixed` snapshot as the incident's healthy sample
(`healthy/reference` is `closure`). The unverified-cause `no_recorded_repair` item now
means only "no recorded repair touched this cause", and
`reflection_no_repair_count` counts those items; Go no longer creates
missing-observation gaps. On partial and failed outcomes (and for trust credit) a
cause is still verified when each written target's own primary finding and each
observed symptom cleared after the last write, the cause owns its resources
exclusively, and an owned child write (`owners` chain, LEARN2/LEARN2B; unchained
Pod/ReplicaSet/Job writes bound every cause, FIX14A) cleared after it. Worker-reported
owner chains are dropped at ingest. The worker
receives compact cause indices and only those causes, verified receipts and typed
applications/diagnoses and scoped cold-verification evidence. Opaque tool transcripts and unattributed playbook/drift
claims are omitted from that learning view. No global recovery, positive fixed
capture, program trust credit, or repaired peer is inferred from partial repair.
A cause with no recorded successful repair is not saved. Full-success reflection follows the
LEARN3 rule: any recorded successful repair on a verified-healthy closure is saved.

Every closed incident with a resumable responder session requires reflection,
including failed, cancelled and self-healed incidents with no verified repair.
The worker resumes that session by default, independently of the response budget
and benchmark cleanup. Settled corpus work does not bypass this dispatch.
Without a verified repaired cause, reflection may return `no_change` with a
concrete no-recorded-repair reason or typed SDK gaps; it cannot publish an `updated`
README or executable claim. `decline_reason=no_recorded_repair` also records one verified cause with no recorded repair write on its own resources (an owner-chained write only) beside an `updated` decision; the replayable causes are still learned and the validator no longer demands a program for the recorded one. A responder Job failure preserves its provider checkpoint from the incident tool
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
The agent's own budget ends 120 s before that deadline. A worker whose turn
fails publishes `AgentResult` `status=failed` with a typed `failure_reason`
(`agent_timeout`, `agent_failed` or `result_too_large`) and `error_summary`,
recorded as `learning_decision=failed`; a failed reflection Job without such a
result is a worker crash, not an agent failure.
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
`CLOSURE_FAILURE_REASON_AGENT_TIMEOUT`. Open work that lost its owner is archived
the same way with `CLOSURE_FAILURE_REASON_OWNER_LOST`: a dispatch still RUNNING
past the response deadline plus one minute with no Job, or a finished responder
whose closure never began within the verification timeout plus one minute (for
example, a controller torn down mid-incident and later restarted). The rule runs
at startup, every step and while paused. API uncertainty preserves the binding;
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

### Concurrent shared memory

Publication is serialized by a lock in the accepted repository's Git common
directory, shared by brokers with different proposal roots. Stale proposals
three-way replay onto the current accepted head; the complete shared check and
independent compilation validate the merged tree before publication. A text
conflict or merged-tree validation failure returns `MemoryDiagnostic` rule
`memory.conflict` with typed `memory_conflict`: original `base_commit`, current
`accepted_commit`, `conflicting_paths`, `accepted_changes` (commit, incident ID,
paths), and `validator_errors`. No candidate commit is accepted on rejection.
The durable reflection rejection ledger retains this structure through restart
and resumes the same provider session in a fresh checkout of current accepted
memory. After a memory conflict, three total publication/correction attempts terminate
visibly; ordinary validation retries without conflicts retain their existing cap. Both incidents' intent must survive correction.
Catalog task reads bind to the prepared proposal commit through `SnapshotAt`,
projecting immutable Git objects while other controllers publish newer heads.
The shared accepted checkout requires a filesystem honoring the common Git lock;
proposal, session/corpus and ledger roots remain lane private. Live lane mounts
and remote shared-storage provisioning remain benchmark runner work.

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

Accepted outcomes preserve reflection ownership across controller restart and
adoption of new source or broker-validated health-refresh detectors. The health guard first
requires the running policy to match accepted inputs, then authenticates the
exact frozen closure against its historical broker-accepted outcome. Resuming
that closure performs reflection/publication/ACK bookkeeping; it does not make
another incident health decision. Publication still three-way merges onto the
current accepted head and runs the complete shared memory check. Objective
changes and unrelated policy changes remain stale: `reflection.objective-changed`
(`objective changed`) or `reflection.policy-changed` (`policy changed`) is
recorded in field 52 before ownership is retired. The broker independently
rechecks that boundary under its publication lock and never retries such results.

`OutcomeRecord.reflection_rejections` is broker-appended typed `ProposalRejection`
history, including stable diagnostic rules such as `memory.conflict`, current
accepted commit and conflicting paths. It never changes incident classification
or verification. The ledger retains the original outcome/task identity even
when an annotation advances accepted HEAD. Duplicate results/annotations do not
publish another reflection or repeat a rejection entry.

### Terminal worker Job failures

A failed worker Job is non-retryable for the frozen execution: transport retries
rejoin the existing Job, while only broker validation corrections receive new task
identities. Ordinary terminal Job failure archives a permanent closure failure on
the first observation (`reason=BROKER_ERROR`, rather than `RETRY_EXHAUSTED`),
retaining the original incident evidence. Restart does not re-arm it.
`closure_failure.last_error`, archived failed-closure diagnostics and controller
errors retain Job condition type/status/reason/message plus Job-UID-owned Pod
container (including init-container) termination reason, exit code and short
message. Status text and collections are bounded; missing Pod API evidence is
reported without replacing the proven Job failure. These are Kubernetes status
observations, not Pod logs or Secret reads. Frozen-deadline expiry retains its
separate typed timeout path and existing reflection decision semantics.

A dispatched worker that disappears without a durable result fails with typed,
non-retryable `JobVanishedError` and diagnostic `worker Job vanished`, including
Job and task identities. The executor retains a request-digest-bound immutable
`<job>-execution` ConfigMap after successful dispatch, separate from request
preparation. Recovery uses that receipt to reject re-creation after restart;
API read failures remain uncertainty, not disappearance. A completed result
still follows the existing deadline and worker-quiescence checks after Job TTL.
Existing executions without dispatch receipts retain deadline-based recovery.
