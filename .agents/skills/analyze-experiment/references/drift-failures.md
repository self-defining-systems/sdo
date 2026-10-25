# Drift and toy failure patterns

## Latent drift classification

**Signature:** a `latent` finding or an outcome `drift_events` entry names a matching program while health detectors remain clear, or a changed object is recorded as `unexplained` without an incident.

Join the event's ref, source revision, fields and responder evidence to the current incident. Declared spec drift still requires admitted owned fields and a successful source comparison. The source inventory additionally compares top-level non-secret informer objects against accepted deployment manifests, including initial listings: source-absent application objects, namespace-bound RBAC and deletion-pending declared objects become ordinary latent findings even with every health detector clear. Accepted PodSpec references (volumes/projected, environment refs, ServiceAccounts and image-pull names) and accepted RBAC subjects/roleRefs declare dependency identities; `referenced_dependency_content` gaps preserve unknown contents. References injected only into live objects grant no authority. Generated descendants of source objects, SDO-owned runtime objects, endpoint/event churn and status-only updates remain quiet. Source failure is never absence; the rolling detector-clear baseline never authorizes source-absent objects.

Unexplained inventory drift attaches to the open incident or batches for its own responder. Closure and self-check require removal/restoration or an exact checked benign assessment with a nonempty reason and valid evidence. The existing outcome `drift_events` retain the explicit acceptance; it is not independently verified repair or learning evidence. `ControllerState.accepted_source_drift` retains checked assessments and `accepted_source_drift_revision` binds their application-source content identity. Memory-only commits preserve that identity; changed source, object incarnation, configuration or deletion metadata invalidates acceptance. Unknown and source errors stay unresolved. Historical releases skipped source-absent drift at closure; do not interpret those closures as full latent-fault repairs.

Initial `memory adopt-existing` compiles supplied memory and records `human/adoption/adopted-existing/skipped`; it does not verify health coverage. An inherited broker `health_judge/initial/passed` commit suppresses startup refresh only when its source inputs and objective match. Legacy judge prose or changed inputs require a judge session and validation Job; report that setup model cost separately. Refresh in progress does not consume onboarding's evaluation attempt. A successful refresh may advance accepted health memory before the controller starts: onboarding retains its original install commit, identities and time only across a continuous single-parent chain of validated judge commits with unchanged source and objective. Source/objective changes, merges and unrelated publications reject that exception. Compare the final record to the pending install snapshot, and inspect the accepted judge lineage separately; do not rewrite pending evidence to match a refresh commit.

Controller iteration logs identify `evaluated_detectors`. Each `findings` list contains only the selected detectors' fresh samples. An empty list on a resource-only iteration does not clear an omitted traffic detector; inspect its actual evaluation history and firing timeline before diagnosing a flap. The generated controller's read-only `--evaluate-once --namespace NS --app-root REPO --deployed-manifest-path PATH --source-drift-report /absolute/full.json` prints a compact count and saves the complete inventory findings without starting probers, adopting fields, dispatching workers or writing to Kubernetes.

Reset must cancel and await all Jobs before cleaning capture sessions or checkouts. Fail-soft reset retains accepted outcomes and clears saved controller Pod identity; seed rollback drops benign labels with those outcomes. Compare drift clearance with timely attributed repair receipts before granting mitigation credit. A benign no-op closes as `cleared_without_action` without learning credit. New run metadata marks `responder-drift/v2` accounting: merged reasoning is charged once to responder tokens, and legacy `drift_tokens` is zero. Keep historical recorded costs intact.

## Toy real-cluster tier

Toy runs use `fastloop up --app toy`, the real production controller/broker, and
scripted registrations without the external SREGym checkout. The application and
fault/oracle definitions live in `apps/toy/`. Its `health-check` oracle kind also
checks repaired spec fields, not just pod readiness. The setup timing file
`toy_timings.json` separates cluster creation, image loading, CNI setup,
application deployment and controller readiness.

Require an actual `reflection_commit` in the native ledger before claiming that
a supplied invariant was accepted. A row can have `oracle.success=true` and
`error=null` while `reflection_decision.learning_decision=failed`; kindnet lets
the broker's default-deny isolation canary reach the API, so it refuses program
acceptance. The toy tier therefore uses Calico. For reuse, inspect the later
outcome's `drift.<program>` finding and the program run's `binding_source=hint`.
`--no-checkpoints` skips optional replay export, while preserving real outcomes,
strict receipts, corpus validation and broker decisions.

Routing capture now records actual `ProbeTCP` results for finding Services, then
Services the snapshot shows as suspect (selector with no ready Pod, or an integer
targetPort no selected Pod declares) even when no finding names them, then up
to two ready peer Services, including direct selected-Pod probes at numeric target
and declared candidate ports. Calls use captured Pod IPs in the existing isolated
prober; no credentials or cluster mutation are added. Automatic capture is capped
at 24 probes per phase, two Pods per Service, four backend ports per Pod, 200 ms
per probe and two seconds total. Open recordings live in the existing protected
`open@<phase>-recordings.json`; fixed observations carry `phase: fixed`. Per-phase
`*-routing.done` files prevent resampling an immutable capture. Missing, capped,
or failed reads remain Unknown, not a refused connection.

The builder emits the isolated prober even when an application has no traffic
generators or traffic-consuming detectors. Startup awaits its readiness before
incident capture; disabling synthetic workloads still leaves routing primitives
available. Older builds silently skipped routing capture when that prober was
absent. New captures record `routing-probe-note.json` in the incident corpus with
`capability: routing_probe`, `status: unavailable`, `reason` (`prober_not_configured`,
`read_view_not_live`, `probe_failed`, or `budget_exhausted` when the capture's time
budget skipped targets), the first affected `phase`, and optional error `detail`.
Probe deadlines are seconds, not milliseconds, so a slow prober is waited for rather
than recorded as a closed port. The controller logs the same gap once per incident. A note is
capability evidence, not a TCP result or a routing diagnosis; inspect the
adjacent recordings for actual successes, refusals and read errors.

A routing Match needs a successful candidate-port TCP result and refused faulty
numeric target on selected ready Pods; declarations alone prove no listener.
Healthy-peer replay also needs recorded evidence. Service-targetPort-only
SetField replay retains original direct-Pod TCP observations only when the full
captured Pod object is unchanged. It never reuses Service probes or fixed answers
to prove an effect. The installed routing example exercises this stricter
predicate and complete repair-to-NoMatch replay through the shared compiler gate.

## Rendered deployment lock (LK1, phase 1)

New controllers publish the sixth artifact class at `.sdo/rendered/`: version-1
`lock.json` indexes namespace-scoped objects, source commit, selected manifest
paths, normalized YAML filenames and SHA-256 hashes, plus named regular/init
container digests. YAML retains the applied image reference, including an existing
mutable tag; compare its separate index digest to runtime image identity rather
than treating tag-versus-digest spelling as spec drift. A failed object has an explicit `error` entry. Only Go writes
these files, through the broker after rollout observation and fresh-render checks.

For post-memory-commit rendering failures, compare the Git tree of
`.sdo/rendered/` before and after the `.sdo/` publication. It must be identical;
the lock's `source_commit` need not equal the operational head. Compare application
inputs outside `.sdo/` before calling a lock stale. Other application changes
invalidate evidence until the completed deployment receives a new lock.
`AcceptedManifest(s)`, adoption, drift and `sdo source` read this accepted snapshot.
A cold restart no longer needs the former in-memory healthy image baseline. Source-pinned
images must remain identical. Mutable or built references require trusted
apply/rollout-time evidence in the broker's private `source-evidence` ledger,
bound to the accepted source inputs, workload, container name and regular/init
role. Its `container_images` entries retain both `applied` and `digest`. Optional
`inputs` records the trusted plan's `source_image`, `context_path`, and
`dockerfile_path`; a source image with empty paths explicitly has no local
build. Missing inputs retain the all-source rule. Known builds compare their
context (respecting Docker ignore files), Dockerfile and ignore configuration.
Any intervening accepted source change to these inputs retires that image's
evidence, including an edit followed by a revert. Unaffected images retain
coverage; gapped workloads require fresh trusted deployment evidence. Legacy
flat `images` lists cannot authorize a substitution. Matching current annotations,
readiness or Pod image status alone are insufficient. A missing ledger after
restart creates an explicit per-object gap rather than accepting the live image.
Publication errors retain the last accepted revision or explicitly recover the
broker's advanced HEAD; an empty publication result must never clear it.

Missing lock/indexed YAML, hash mismatch and per-object `error` mean evidence
gap, never absence or health. Inspect successful objects and retryable adoption
coverage separately: a consul image gap must not suppress reservation Service
drift. Lock publication uses an attributed `SDO-Actor: controller`,
`SDO-Phase: rendered` broker commit; a crash before it leaves a gap and retries
rollout observation, while a crash after it carries the accepted lock forward.
No phase-2 diff, quarantine or revalidation behavior is implied.


## Qualified scoped drift

State changes may carry `api_version` and `namespace`; qualify those together with kind/name before joining evidence. An empty namespace on admission webhooks is cluster scope, never the application namespace. Older app-local captures may omit both fields. Do not collapse same-name foreign Deployments from different namespaces.

Named Mutating/ValidatingWebhookConfiguration changes can enter unexplained drift after a successful accepted-source lookup, including proven source-absent creates/deletes. An unavailable source is not proven absence. Qualified read-only lookup still proves application-lock selection/freshness and supplies no foreign image/apply authority. Pending comparisons survive observation gaps; revoked targets are dropped from observation, while scope additions establish initial baselines without synthetic changes. Initial observations are suppressed. Foreign Endpoints turnover supplies qualified baseline evidence and detector wakeups without automatically opening a configuration drift incident. Per-target unobserved-kind strings qualify API version/kind/namespace/name. Failed reads retain the prior comparison baseline and do not manufacture removals.

## Source inventory with rendered evidence gaps

Object presence and generated-owner ancestry use accepted source identities after
validating the lock's selection and application-source freshness. Missing image
receipts or per-object rendered bytes do not remove declared identities or block
this inventory check. The read-only report includes `evidence_gaps` entries with
`kind: rendered_object`, the object `ref`, and a `reason`; its compact summary
counts them separately from unexplained objects. The live controller logs these
typed gaps when loading its cached inventory. Zero inventory findings with gaps
proves only the presence/ownership check, never image equality or global health.
Image-field comparison still requires the durable rendered evidence, and an
unavailable, mismatched or stale lock still prevents inventory verification.


## Verified onboarding acknowledgement

`namespace_onboarding` in controller state and the incident request (also the reflection closure request) is a separate identity list, with namespace, accepted commit and establishment time. Native install/adoption freezes identities in immutable `sdo-namespace-onboarding-pending` without host health probes. Startup rendered-lock reconciliation follows the onboarding evaluation, keeping its accepted install commit stable. The in-cluster controller creates `sdo-namespace-onboarding-attempt` before its first confirmed complete evaluation, then publishes immutable `sdo-namespace-onboarding` only when every detector is clear, none errors, the accepted commit is unchanged and no incident exists. A non-clear evaluation or crash leaves pending evidence without authority; later healthy periods and restarts never retry it. Logs report `install_to_record`; `onboarding.json` is ProtoJSON. Reinstall, controller restart and memory-only publication retain the original list and never recapture. No Secret API is queried. Acknowledged pre-existing objects and their owner descendants are not unexplained merely because they lack manifests; contents remain unverified and receive no repair or learning credit. Later top-level objects and relevant RBAC still block closure. Source-declared RBAC changes and deletion checks remain active.

For a read-only replay, export the installer's `onboarding.json` and pass `--source-drift-acknowledgement /absolute/ack.json` alongside the existing evaluator flags. Its report includes the same reusable `namespace_onboarding` object and typed dependency/render gaps. Missing acknowledgement grants no exception: never manufacture it from incident-time inventory or a rolling healthy baseline. A zero finding count remains inventory evidence only, not global health proof.


Probe windows still collecting confirmation samples appear as detector status `pending`, never `clear`, and do not consume the one onboarding attempt. The final immutable ConfigMap also contains `reachability-reference.json`, published atomically with `onboarding.json`. It records configuration-bound failed TCP samples for ports refused throughout an independently goal-healthy install window. These are typed `declared-but-never-served` observations in controller logs, not findings or verified repairs. A prior successful dial prevents capture; a later successful dial or target configuration change invalidates an exclusion. Three default probe rounds add 20 seconds of sample collection to install-to-record, plus probe/scheduling time, without extending the normal three-failure incident path. Reference data is restored from the finalized ConfigMap; pending identities and incident-time state cannot manufacture it.
