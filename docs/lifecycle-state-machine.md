# Controller lifecycle state machine

The durable schema is `sdo/contracts/proto/sdodev/contracts/v1alpha1/controller_state.proto`.
The controller exports and restores the generated `ControllerState` directly;
there is no second persistence struct. Canonical ProtoJSON uses enum names.
Unknown names and undefined numeric enum values fail validation. This is a
breaking format change; old phase strings require an operational cutover.

## States, events and effects

Responder dispatch phases: unspecified/idle, admission deferred, workspace
pending/running, pending/running, completed. Admission holds the incident's
identity and findings. Workspace preparation and responder dispatch are separate
effects. Completion retains results for independent verification; timeout is a
typed worker outcome, while API/transport failure rejoins the same worker.
Successful verification freezes the closure.

Closure phases: unspecified, pending/processing, committed/acknowledging, failed.
Its nested reflection sequence is dispatch, worker result, format, independent
validator, broker publication, done or failure. Outcome publication precedes
reflection. Validation rejection may dispatch a bounded correction;
infrastructure failure retries the same work. A committed closure waits for any
required controller rollout before acknowledgment and worktree release.

Events: incident opened, admission reassessed, workspace prepared/rejected,
responder completed/timed out, detector observation, verification elapsed,
closure frozen, reflection result, format result, validator accepted/rejected,
broker accepted/rejected, acknowledgment result, API fault, leadership loss,
controller restart and clock advanced. Events carry incident/effect identity;
clock-dependent decisions consume the event timestamp. Effects are plain values:
prepare workspace, dispatch agent, format proposal, validate proposal, publish
commit, acknowledge closure and release workspace. The driver owns contexts,
clocks, Kubernetes, files and Git.

Snapshots convert in-flight workspace/dispatch phases to pending, and closure
processing/acknowledging to pending/committed. Restart reuses persisted requests
and completed results. The persisted incident view is for readers; restore
ignores it and publishes fresh observations.

## Invariants and checking scope

* Go owns decisions and accepted commits; Python transports generated contracts.
* The broker alone accepts operational artifacts, with independent validation.
* A pure transition consumes only state and event, never wall time, RNG or I/O.
* Effect identity and request bytes depend on incident, accepted commit and step.
  Retrying an effect cannot alter its deadline or discard a completed result.
* A permanent failure belongs to one incident and must allow the next incident.
* Diagnosis precedes mitigation, with at most one graded mitigation per incident.
* Child deadlines fit the remaining parent budget with headroom.
* New faults observed during an incident survive its acknowledgment.

The controller and native reflection coordinator now call
`runtime/lifecyclecore.Step` and `StepReflection`. Their only inputs are generated
state and plain events; effects contain values, never contexts or I/O interfaces.
The declared core passes the [enforced purity gate](go-purity.md).

The closure explorer covers two incidents, three transient faults and one
restart: 457 reachable states and 931 transitions. The reflection explorer covers
34,046 states and 75,166 transitions with three API faults, one restart and three
bounded worker attempts. Every transition is repeated and compared byte for byte;
inputs must remain unchanged. The checks cover stable retry identities/payloads,
completed-result retention, bounded correction, reset only after typed rejection,
and a path to terminal outcomes from every reachable state under bounded faults.
Mutation helpers demonstrate rejection of time-dependent retry payloads,
non-determinism, lost results and failure that blocks the next incident. Each
explorer has a two-minute wall cap; the existing chaos CI job runs them with a
five-minute process timeout.

Publication remains one trusted broker effect containing gofmt, independent
validator Jobs and commit. The explorer does not prove that effect's internal
ordering; broker validation/transport tests cover it. This preserves the broker
boundary for the later extraction in the table below. Incident-response ordering
and real-driver seeded simulation (slices 3–4) are explicitly deferred by the user
to conserve the shared account quota; these models do not prove them.

## Durable retry and failure evidence

A closure error consumes the retry budget frozen at that incident's first failure; restart configuration cannot enlarge it. Shutdown/leadership cancellation
does not. Agent deadline errors and invalid receipts have typed reasons; exhausted
retries become `CLOSURE_FAILURE_REASON_RETRY_EXHAUSTED`. The failed closure and
reason remain available, while later incidents can open.

`ControllerState.failed_closures` is an archive outbox. Before accepting the next
main-state CAS, the private ConfigMap adapter writes every queued record to a
content-addressed `sdo-failed-closure-*` ConfigMap (`failed-closure.json`, label
`sdo.dev/closure-archive=failed`). It omits the completed outbox from the main
ConfigMap. New workspace/dispatch effects still require confirmed state durability,
so an archive failure cannot discard evidence or authorize a later effect. The
archive is runtime evidence outside `.sdo/`, not a new operational-memory class.
A crash after archive creation but before the root CAS safely replays the write;
distinct terminal observations retain distinct content-addressed records.

`BrokerLedger.reflection_phase`, `reflection_workspace`, `reflection_paths` and
`reflection_dispatch_at` are the nested reflection state. `reflection_results`
retains completed decisions even when a typed rejection resets a checkout.
Infrastructure faults never request reset. A restart rejoins the same task even
when the completed checkout has edits but its decision was not yet persisted.

The full `AgentTask` is atomically frozen outside the checkout under the native
ledger's `reflection-tasks/` directory before worker execution. Retries read it
before consulting changing memory/configuration. IDs keep the previous happy-path
format; corrections get distinct attempt IDs. Reflection deadlines derive from
the persisted dispatch event timestamp plus the configured budget; a transport
retry cannot rebuild or extend an attempt's deadline. A typed correction is a new
attempt with a distinct identity. General child-budget/headroom
enforcement remains follow-up work.

## Further functional-core extractions

| Subsystem | Pure core to extract | Private edge under that subsystem's `internal/...` |
|---|---|---|
| Broker | Ownership, commit eligibility, rejection/correction transitions and accepted-revision decisions | Git/worktree mutation, locks, validator Jobs and durable commit receipts |
| Memory | Logical artifact projection, schema validation, snapshot selection and content identities | Repository reads, physical layout, outcome persistence and migration |
| Late findings | Report selection, digest/correlation rules and pull-budget transitions | HTTP transport, snapshot observation and report persistence |
| Runtime controller | Scheduling, batching, incident/closure transitions and deadline calculation | Kubernetes caches/Jobs/ConfigMaps, timers, telemetry, leader election; lifecycle persistence is already `runtime/internal/lifecycleio` |
| Program learning / fast path | Trust credit, revision binding, match eligibility, one-attempt and verification transitions | Live snapshots, action application/rollback, capture replay and attempt-marker storage |

Each core accepts plain events and returns decisions/effects. Other subsystems
send events through its driver. Internal visibility prevents outsiders importing
the edge; depguard must also prevent the core importing its own edge. Other
subsystem extractions are follow-up work, not part of this task.
