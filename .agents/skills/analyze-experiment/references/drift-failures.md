# Drift and toy failure patterns

## Latent drift classification

**Signature:** a `latent` finding or an outcome `drift_events` entry names a matching program while health detectors remain clear, or a changed object is recorded as `unexplained` without an incident.

Join the event's ref, changed fields and old/source/new values, manager/time, program ID and responder evidence to the current incident. Declared drift requires changed `sdo-deployer` fields and a successful source comparison. Foreign drift requires positively established accepted-source absence, a reviewed namespace-local NetworkPolicy and no controlling owner. Metadata-only updates, initial listings, controller churn and foreign deletes must not open incidents. Source failure is never absence.

Unexplained drift is an ordinary `drift.unexplained` question, attached immediately to an open incident or batched for its own responder. Require a concrete source contradiction plus violated relation or observed impact before mutation; source absence and missing port declarations alone do not establish fault. Quiet health never clears retained drift. Source restoration/adoption or a checked exact benign label may clear unexplained fields; learned `drift.<program>` findings need explicit NoMatch on every retained binding, including deleted objects. Unknown, missing bindings, errors and timeouts remain unresolved.

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

Routing capture now records actual `ProbeTCP` results for finding Services and up
to two ready peer Services, including direct selected-Pod probes at numeric target
and declared candidate ports. Calls use captured Pod IPs in the existing isolated
prober; no credentials or cluster mutation are added. Automatic capture is capped
at 24 probes per phase, two Pods per Service, four backend ports per Pod, 200 ms
per probe and two seconds total. Open recordings live in the existing protected
`open@<phase>-recordings.json`; fixed observations carry `phase: fixed`. Per-phase
`*-routing.done` files prevent resampling an immutable capture. Missing, capped,
or failed reads remain Unknown, not a refused connection.

A routing Match needs a successful candidate-port TCP result and refused faulty
numeric target on selected ready Pods; declarations alone prove no listener.
Healthy-peer replay also needs recorded evidence. Service-targetPort-only
SetField replay retains original direct-Pod TCP observations only when the full
captured Pod object is unchanged. It never reuses Service probes or fixed answers
to prove an effect. The installed routing example exercises this stricter
predicate and complete repair-to-NoMatch replay through the shared compiler gate.
