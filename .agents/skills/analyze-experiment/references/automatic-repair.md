# Agent-selected repair evidence

Go diagnoses at incident open and records `IncidentRequest.playbook_coverage`; outcome field 35 copies that snapshot. The current ordered pipeline may finish through accepted deterministic repairs or hand remaining findings to a responder. Join `resolution_path` and `prior_stages` to immutable application evidence; historical `fast_path_*` fields belong to the retired execution path.

`PlaybookCoverage.matches` contains every Match (never NoMatch, Unknown or NeedsJudgement), deduplicated by logical playbook ID, program revision and canonical object/value bindings. Match fields are `playbook_id` (1), `program_revision` (2), repeated existing `Binding` messages (3), sorted `explained_finding_fingerprints` (4), `can_mitigate` (5), deterministic menu option ID `mitigation_selection` (6), `verified_successes` (7) and `verified_failures` (8). The option ID selects the same binding from a fresh menu; it grants no write authority. Retired playbooks are excluded by the loader. Counts are information and do not filter matches.

Explained findings require their exact diagnosis fingerprint and an explicit authored playbook link or the controller-owned `drift.<program>`/rule identity. Primary, related, parameter-bound and direct workload relations corroborate that link; shared resources or candidate aliases alone never explain a finding. Aggregate Match evidence without a resource still offers the playbook. `unexplained_finding_fingerprints` lists the sorted remaining active findings. A finding may be explained by multiple programs; all matches remain available.

Coverage is diagnosis, not proof that a program fixed a finding. Join its bindings and explained fingerprints to controller-recorded `plan_applications`, `program_tool_calls`, `repair_actions`, and independently verified final findings. Program options and raw agent actions have separate program IDs (`action:<type>` for raw menu actions). Use executed receipts to attribute repairs; do not count a Match as a repair.

Failed verification retains landed writes. Only explicit responder rollback can undo a completed apply; compensation within a partially failed write batch remains. Defaults are 10 loops, 5 validation retries, 5-minute verification and the enabled mitigation menu. Historical automatic-repair evidence must be interpreted against its original code revision.

Menu applications also appear as typed `plan_applications` with their selected program ID, or `action:<type>` for raw actions. Per-application `program-plan` receipts and per-action `mitigation-menu` receipts can describe the same write at different granularities; count applied actions once, correlated by application/attempt and resource, rather than summing receipt list lengths.

Scoped mitigation checks retain original target fingerprints independently of written objects (`mitigation_health.targeted_finding_fingerprints`). A peer finding on the same resource does not block that scope. Clearing persistence still gates closure, but absent fingerprints are omitted from `still_broken`. Immutable completed application receipts survive a tool-state restart before any continuation and reach the final incident repair receipts; plans and rollback preimages do not.

## Ordered response stages

A learned finding that activates after dispatch re-runs the stage on the responder's loop: the `deterministic` report in `prior_stages` is one report that gains attempts (a retryable skipped attempt is redone in place; an attempt that wrote or has terminal `plan_not_executable` is never repeated), and the responder's tool reply carries `prior_stages` notes once. Compare an attempt's apply time with the finding's first activation to see the mid-incident pass. Capture recordings (`open@detect`, `open@pre-apply`) hold one `DiffAgainstSource` per written resource; a missing acceptance record is recorded with error `acceptance_record_unavailable: ...` and the declared-field result.

The default Go path now runs `deterministic,agent`; `agent` alone preserves the baseline. `prior_stages` records each automatic attempt, its exact diagnosed finding fingerprints, program revision/bindings, real plan/application receipts and bound post-apply diagnosis. An opening incident finding can outlive its detector's presence in the health-only responder registry. A completed scoped verification plus bound post-write NoMatch can discharge that attempt's exact live diagnosis targets; a fresh finding with the same fingerprint still blocks it. Retained opening findings alone do not override that newer proof.

The stage applies all its matched plans in one apply with one rollout wait, inside one 45 s stage budget (checks, apply, rollout wait and this observation); matches it could not apply in time are `skipped` with `stage_budget`. After every opening fault is repaired, newly observed health findings may consume what is left of that budget in bounded observation before handoff. They are retained on timeout; no closure, policy or objective authority is transferred. Findings are assigned their emitting detector identity at `sdk.DetectObserved`, including responder verification and handoff.

Deterministic completion retains real `plan`/`apply` transcripts and performs the ordinary responder final-equivalence check before projecting applications into the outcome. Durable responder rejoin retains those typed stage applications and repaired causes without rerunning automatic writes. Failed-outcome reflection still requires the existing timed, scoped application/NoMatch/final-equivalence proof. Successful closure still requires fresh independent detector clearance. Neither `applied_cleared` text nor a NoMatch alone authorizes reflection publication. Broker objective/policy supersession rules remain authoritative.

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

Source reconciliation action reports carry `source_reconcile`: the object reference, `converged`, `residual_paths`, `observation_error`, and `additions_known`. Paths contain no values. Status `partial` with `failure_reason=source_residual` means applied writes remain useful but readback still differs or is unavailable; it supplies no independent verification credit. `additions_known=false` limits comparison to declared source fields because no bound acceptance record is available. A recreated object that still carries its earlier instance's `sdo.dev/accepted-paths` record keeps it as its acceptance point (rebound to the new UID); with no record to carry, adoption accepts undeclared scalars equal to the server's dry-run render of accepted source (probe name `<name>-sdo-default-render`). Older runs reported server defaults such as `spec.progressDeadlineSeconds` or `terminationMessagePath` as permanent residuals after such a recreate. Deployment rollout residuals include active old-template ReplicaSets and their remaining Pods as `rollout.replicasets[namespace/name]` and `rollout.pods[namespace/name]`; zero-sized history without Pods is excluded. Reconciliation only observes these remnants; the Deployment controller and responder own rollout handling. The retained wire field `rollout_steps` is no longer produced. Normal rollout completion remains separate evidence.

Live diagnosis bindings come from `program.BindFinding` through `SelectProgramCandidates`, shared by stage selection, matched menu selection and reflection repeat observation. Stage target associations are separate from authored explanation coverage: an unlinked live Match remains applicable. Optional roles may be absent and scalar defaults need no explicit value entry; historical `unbound_parameter` skips can reflect the retired duplicate coverage/completeness gate, not a missing declared parameter.

`plan_not_executable` is a typed terminal skip for a structurally invalid learned plan (including an empty plan or incompatible raw patch/list edits). The common plan builder returns `NonExecutablePlanError`; accepted program/revision failures are remembered before another binding can prepare them, and the durable stage report restores that decision across refresh and restart. No writes are attempted for it; remaining work hands over to the agent. Observation, timeout and execution errors do not become structural terminal decisions merely because planning failed. This is runtime handling, never a replay publication gate.
