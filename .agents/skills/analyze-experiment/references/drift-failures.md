# Drift and toy failure patterns

## Latent drift classification

**Signature:** a `latent` finding or an outcome `drift_events` entry names a matching program while health detectors remain clear, or a changed object is recorded as `unexplained` without an incident.

Join the event's ref, changed fields and old/source/new values, manager/time, program ID and responder evidence to the current incident. Declared drift requires changed `sdo-deployer` fields and a successful source comparison. Foreign drift requires positively established accepted-source absence, a reviewed namespace-local NetworkPolicy and no controlling owner. Metadata-only updates, initial listings, controller churn and foreign deletes must not open incidents. Source failure is never absence.

Unexplained drift is an ordinary `drift.unexplained` question, attached immediately to an open incident or batched for its own responder. Require a concrete source contradiction plus violated relation or observed impact before mutation; source absence and missing port declarations alone do not establish fault. Drift findings never gate closure: the incident closes when the health detectors clear, and drift is not re-checked after the responder completes.

Only checked benign labels in existing outcomes suppress identical current/source fields and manager; timestamps, old values and unrelated outcome commits do not change identity. Bug and unsure labels never suppress a repaired recurrence. Inspect `pending_batch`, the moving incident view and late-findings activations when drift arrives after launch; it joins the same incident and must block closure until resolved. New keys bypass repeat drift dispatch/token budgets; health dispatch and evidence attachment never wait. Historical investigator state/verdict ConfigMaps are evidence from retired releases, not current controller inputs.

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
