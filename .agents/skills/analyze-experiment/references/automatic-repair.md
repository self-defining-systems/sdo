# Agent-selected repair evidence

Go diagnoses at incident open and records `IncidentRequest.playbook_coverage`; outcome field 35 copies that snapshot. The current ordered pipeline may finish through accepted deterministic repairs or hand remaining findings to a responder. Join `resolution_path` and `prior_stages` to immutable application evidence; historical `fast_path_*` fields belong to the retired execution path.

An opening diagnosis that fails (an Unknown whose `error` comes from a failed diagnosis: a read that did not answer, a Diagnose error, a deadline, an unreachable memory host) is asked again by the deterministic stage for that program only, up to three diagnoses one second apart, before coverage is built; an Unknown the program answered itself, explained or not, is final. `initial_program_runs` keeps the failed runs in order and appends the later answer for that program and binding (the `open@detect` capture holds only the first diagnosis's reads). A run's `error` names the read that failed with its cause (for example `external observation unavailable: exec transport failed: context deadline exceeded`); runs recorded before this change carry the class without the cause.

`PlaybookCoverage.matches` contains every Match (never NoMatch, Unknown or NeedsJudgement), deduplicated by logical playbook ID, program revision and canonical object/value bindings. Match fields are `playbook_id` (1), `program_revision` (2), repeated existing `Binding` messages (3), sorted `explained_finding_fingerprints` (4), `can_mitigate` (5), deterministic menu option ID `mitigation_selection` (6), `verified_successes` (7) and `verified_failures` (8). The option ID selects the same binding from a fresh menu; it grants no write authority. Retired playbooks are excluded by the loader. Counts are information and do not filter matches. Each match also carries `retrieval` (9): how the playbook came to be tried on the finding (`detector`: a detector finding names it; `role_less_scan`: a role-less playbook tried on every finding; `role_bound`; `credited`; `suspect`), the same value as `program_runs[].retrieval` (11). It is attribution only: a role-less playbook is still tried on every finding. `PlaybookCoverage.unresolved` (3) lists the tried playbooks whose diagnosis was Unknown or NeedsJudgement as `PlaybookVerdict` (`playbook_id`, `program_revision`, `result`, `reason` (the error or evidence, at most 300 bytes), `retrieval`, `unknown_on_recurrence`), so a playbook that cannot recognise its fault is visible next to the matches. `prior_stages[].diagnoses[]` (`StageReport` field 9) is the same `PlaybookVerdict` for every playbook the stage diagnosed (best verdict per playbook and revision, the built-in source restore excluded), match or not. `ProgramTrust.unknown_on_recurrence` counts the outcomes whose `program_runs` hold an Unknown for the playbook; it is a counter that gates nothing and never adds a failure. The responder's `sdo detect` header states them as `tried by sdo: <id> -> match (<retrieval>)` and `tried by sdo: <id> -> unknown (<reason>)`.

Explained findings require their exact diagnosis fingerprint and an explicit authored playbook link or the controller-owned `drift.<program>`/rule identity. Primary, related, parameter-bound and direct workload relations corroborate that link; shared resources or candidate aliases alone never explain a finding. Aggregate Match evidence without a resource still offers the playbook. `unexplained_finding_fingerprints` lists the sorted remaining active findings. A finding may be explained by multiple programs; all matches remain available.

Coverage is diagnosis, not proof that a program fixed a finding. Join its bindings and explained fingerprints to controller-recorded `plan_applications`, `program_tool_calls`, `repair_actions`, and independently verified final findings. Program options and raw agent actions have separate program IDs (`action:<type>` for raw menu actions). Use executed receipts to attribute repairs; do not count a Match as a repair.

Failed verification retains landed writes. Only explicit responder rollback can undo a completed apply; compensation within a partially failed write batch remains. Defaults are 10 loops, 5 validation retries, 5-minute verification and the enabled mitigation menu. Historical automatic-repair evidence must be interpreted against its original code revision.

Menu applications also appear as typed `plan_applications` with their selected program ID, or `action:<type>` for raw actions. Per-application `program-plan` receipts and per-action `mitigation-menu` receipts can describe the same write at different granularities; count applied actions once, correlated by application/attempt and resource, rather than summing receipt list lengths.

Scoped mitigation checks retain original target fingerprints independently of written objects (`mitigation_health.targeted_finding_fingerprints`). A peer finding on the same resource does not block that scope. Clearing persistence still gates closure, but absent fingerprints are omitted from `still_broken`. Immutable completed application receipts survive a tool-state restart before any continuation and reach the final incident repair receipts; plans and rollback preimages do not.

## Ordered response stages

During controller-owned recovery, an activation or clear can request another bounded automatic pass before the recovery deadline. A newly linked finding can also queue a pass during the opening stage. The one `deterministic` report in `prior_stages` merges later attempts and preserves existing receipts. Retryable skips can run again. Attempts that wrote or have terminal `plan_not_executable` stay final. Compare apply time with finding transitions to identify a follow-up pass. Handoff waits for running passes and opening-stage queued evidence. After handoff, `sdo detect` shows historical verdicts and labels fresh matches from prior non-matches or Unknowns with `match now`. Capture recordings (`open@detect`, `open@pre-apply`) hold one `DiffAgainstSource` per written resource; a missing acceptance record is recorded with error `acceptance_record_unavailable: ...` and the declared-field result.

The default Go path now runs `deterministic,agent`; `agent` alone preserves the baseline. `prior_stages` records each automatic attempt, its exact diagnosed finding fingerprints, program revision/bindings, real plan/application receipts and bound post-apply diagnosis. An opening incident finding can outlive its detector's presence in the health-only responder registry. A completed scoped verification plus bound post-write NoMatch can discharge that attempt's exact live diagnosis targets; a fresh finding with the same fingerprint still blocks it. Retained opening findings alone do not override that newer proof.

The stage applies and publishes receipts, then controller closure owns recovery and dispatch. `apply_completed_at` anchors the fixed `recovery_deadline`. `stability_deadline` optionally records the one extension for a fresh clear waiting only for stability. `handoff_selected` and `handoff_keys` preserve the controller's residual alert decision across rejoin. `RecoveryWait.CLEARED` is set by independent closure. `rollout_unsettled_at_deadline` records a terminal revision-bound failure even if the rollout eventually settles.

A new automatic application can earn credit only on verified success with a single effective application and no prior handoff or agent change. Bound post-apply NoMatch and final effect evidence are still required. Interpret older receipts with their original stage semantics.

Admission-denied Pod creation ends a rollout wait immediately. A stage mitigation
with `result=admission_blocked` retains its applied repair; its
`apply_receipt.rollout_pending` has `reason_code=admission_blocked`, named
`admission_webhooks`, resolved `admission_configurations` and owned workload
failure evidence. This is an unverified prerequisite, never automatic rollback
or independent repair credit. Agent applies use the same classification.

New requests can carry `prior_response_progress` from failed episodes with an
exact overlapping detector/finding fingerprint since the last healthy outcome.
It contains successful action receipts and observed blockers, as information
for avoiding duplicate work; it neither credits the new outcome nor restores
mutation authority. Full receipts remain in the private incident facts file.
The handoff also retains the existing
`controller.rollout/deployment-rollout-pending` finding identity for a recorded
admission prerequisite, including stage rejoin. Old Ready replicas clearing
health cannot silently skip the responder; fresh controller closure observation
still owns recovery verification.


Prior-response progress is a bounded informational projection (at most eight
newest episodes and 32 KiB before task encoding); transport pressure can compact
it further. `ResponseProgress.truncated` and `omitted_episodes` explicitly mark
loss. Complete receipts remain in the durable outcomes, not necessarily in the
next request's facts file. Admission prerequisites require current-generation
owned evidence newer than the rollout observation/wait start; retained warnings
remain diagnostics and cannot alone abort a later scale-up.

Source reconciliation action reports carry `source_reconcile`: the object reference, `converged`, `residual_paths`, `observation_error`, and `additions_known`. Paths contain no values. Status `partial` with `failure_reason=source_residual` means applied writes remain useful but observed residual paths remain; it supplies no independent verification credit. Unavailable readback with no observed residual paths remains pending and unverified, with `rollout_pending` naming the observation gap. The stage reports `rollout_pending_at_handoff`; later observation can settle the same application without another write. Older builds reported this observation gap as `partial` / `rollout_failed`; do not infer that the write failed from those rows. `additions_known=false` limits comparison to declared source fields because no bound acceptance record is available. A recreated object that still carries its earlier instance's `sdo.dev/accepted-paths` record keeps it as its acceptance point (rebound to the new UID); with no record to carry, adoption accepts undeclared scalars equal to the server's dry-run render of accepted source (probe name `<name>-sdo-default-render`). Older runs reported server defaults such as `spec.progressDeadlineSeconds` or `terminationMessagePath` as permanent residuals after such a recreate. Deployment rollout residuals include active old-template ReplicaSets and their remaining Pods as `rollout.replicasets[namespace/name]` and `rollout.pods[namespace/name]`; zero-sized history without Pods is excluded. Reconciliation only observes these remnants; the Deployment controller and responder own rollout handling. The retained wire field `rollout_steps` is no longer produced. Normal rollout completion remains separate evidence.

Live diagnosis bindings come from `program.BindFinding` through `SelectProgramCandidates`, shared by stage selection, matched menu selection and reflection repeat observation. Stage target associations are separate from authored explanation coverage: an unlinked live Match remains applicable. Optional roles may be absent and scalar defaults need no explicit value entry; historical `unbound_parameter` skips can reflect the retired duplicate coverage/completeness gate, not a missing declared parameter.

`StageReport.handoff_state` describes the applied repairs when a freshly failing member of the alert set triggered handoff. Residuals are `ACTIVE`. `PROBE_LAG`, `BASELINE_CLUE`, and `observe_only` apply to historical runs.

A learned playbook that was credited for (symptom, component) pairs (`runtime.ProgramCreditIndex`, derived at load from `success` outcomes, never stored) is bound by them in the automatic stage: for a pair whose symptom is in `IncidentRequest.opening_keys`, a resource a finding names (primary or related) equal to the pair's component binds the playbook's `component` role, provided every other parameter binds too; the run carries `binding_source=credited` and the fingerprint of the finding that named the suspect. For such a playbook the stage never uses the finding binding, so a same-Kind suspect without its own pair is not bound, and of several credited Matches only the first in suspect order (the order findings name resources) is kept; NoMatch and Unknown runs remain as evidence. The responder's own diagnosis (`sdo detect`, dispatch preparation) still includes the finding-bound candidates, so a playbook can be offered on a suspect it is not yet credited for. A playbook with no credited pair keeps today's finding binding, so memory that predates the key is not stranded.

Programs declaring object roles need at least one finding-bound object or an explicit playbook hint to be
candidates. Historical empty optional-role diagnoses often returned Unknown on unrelated findings; count
affected repeats separately from repeats with an actually blocked repair. Initial runs are opening evidence;
outcome runs also include later diagnoses, aliases and post-repair stale bindings, so do not add their
counts as distinct failed attempts. Unknown without evidence/error has an unrecorded reason.

Older contract projections dropped resource-free evidence unless its fact was exactly `diagnosis`.
Consequently an empty wire Unknown can be a lost binding/catalog/relation explanation rather than a failed
primitive. New projections carry all resource-free Unknown facts through `program_runs[].error`, keeping
the scoped evidence contract intact. These are diagnostic reasons, never evidence of health or a Match.

The automatic stage reserves rollout time for remaining candidates and continues after an application
failure; every later binding diagnoses fresh state through the ordinary executor fences. Before this change,
one failure could hand off otherwise matched repairs without attempting them. Compare stage receipt
completion times with model tool invocations: the fastloop mitigation timestamp can be the last successful
repair, whereas an earlier independently verified stage repair is already a first fix. Model turns require
provider response IDs, not one count per controller tool call (one model response can execute several).

Historical `wait for the cache to observe plan writes: write observation unknown` on a confirmed
ResourceQuota delete can mean the barrier waited for a nonexistent informer. The current barrier requires
cache catch-up only for watched kinds; an uncached kind needs the executor's fresh, process-local direct
effect proof. Persisted receipts do not restore that proof. This removes the generic API operation failure
class without treating missing observations as success or bypassing watched UID/version barriers.

`ineffective` skips a match whose program revision already ended `applied_still_matching` in this incident for the same finding fingerprints (a replacement object, such as the Pod a restart creates, is a new binding but not new evidence; a finding that attempt did not target is). A deferred second diagnosis turn runs only after a turn that wrote or cleared something.

Historical `reverted_ineffective` and `kept_reason` were produced by the retired automatic rollback path. Current automatic writes stay applied. Executor compensation and explicit responder rollback still have receipts.

`plan_not_executable` is a typed terminal skip for a structurally invalid learned plan (including an empty plan or incompatible raw patch/list edits). The common plan builder returns `NonExecutablePlanError`; failures bind to the program revision and complete object/scalar assignment, and the durable stage report restores that binding's decision across refresh and restart. Other bindings remain eligible. Older builds cached by program/revision and could skip a real webhook after a phantom binding failed. No writes are attempted for the refused binding. Observation, timeout and execution errors remain retryable. This is runtime handling, never a replay publication gate.

Before structural validation, learned `SyncWebhookCA` plans can fill missing Secret reference fields from
one exact `AdmissionWebhooksFor` configuration/entry/serving-key relation, including through working-program
pipes. Explicit fields are never overwritten; ambiguous or absent relations remain errors. No Secret read,
key guess, program-name exception or extra mutation authority is added. Inspect the final bound plan when
comparing older missing-Secret-parameter failures with current stage execution.

## Source departures and the built-in `source-restore` playbook

The `change:` line of a suspect in `sdo detect` comes first from `runtime.SourceDepartures`: a non-SDO manager's change that still differs from accepted source, with who, when and the `sdo mitigate ReconcileFromSource|CreateFromSource|DeleteObject --bind ref=Kind/ns/name` command on the expand line. An object accepted source declares that is gone is `missing` (CreateFromSource); a `ReconcileFromSource` whose target is absent runs as a create, so its plan application records `create_from_source`, an absent pre-image and a UID-bound create effect. The built-in `source-restore` playbook (`--bind component=NAME`) diagnoses and restores the same departures (non-SDO, made before the onset) for any component on a failing path, also one a passing path shares, and for the opening condition's own component at any path position, dependency hops included. In the automatic stage only, it skips a departure whose drift event is `seen_healthy` (observed before the controller's last settled healthy stretch began, `healthy_since` in controller state), so such a change shows as `diagnose_no_longer_matches` there while the responder can still restore it. It appears in a stage report as an attempt with `playbook_id: source-restore`; by default (`response.builtin_restore_auto` on) the stage applies it like a matched learned playbook; with the switch off the attempt has `skip_reason: builtin_restore_manual` and the responder may `sdo mitigate` it. It is never learned: it earns no credited pair or trust, and its writes create no reflection work. Secrets show metadata only and have no command. A ConfigMap read only at container start (a subPath mount, an env `valueFrom` or an `envFrom`) by a workload that runs a Pod started after the foreign change stays a departure after it matches source again: `not_reloaded`, naming those workloads, with `sdo mitigate RestartRollout` commands. `source-restore` restores the object and then restarts those workloads in the same plan, never a data store's (`holds_data` or a data role in the map); its post-apply Diagnose clears only when every such Pod started after the write, so `applied_cleared` means the new Pods run the restored content.
