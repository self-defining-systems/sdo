# Drift detection and learned invariants

Status: current design (2026-10-21). The responder merge is implemented; D1–D3 use the accepted full-merge choices below. Builds on [playbook-programs.md](playbook-programs.md).

## Problem

The original incident path depended on health detectors. The current drift path also catches declared-field changes, but source-absent objects need explicit admission. A latent fault can change configuration while the accepted health objective stays quiet. Examples are a Service `targetPort` that no pod serves in an app that routes through Consul, or an edited selector on an unused path. Live runs showed the consequences:

- The no-memory baseline mitigated 2 of 15 target-port faults.
- SDO caught one only when a learned program happened to be swept during an unrelated incident.

Live state also legitimately diverges from source all the time: autoscalers, webhooks, progressive delivery, operational toggles, hotfixes, and SDO's own `recorded-actions` repairs. So "live differs from source" is a question, not a fault.

## Goals

- Detect latent faults deterministically, within seconds of the change, for every fault class SDO has an invariant for.
- Distinguish faults from legitimate drift without a human in the loop.
- Learn only from evidence. New invariants come from verified incidents and investigated drift. Benign drift turns into expected-drift rules and tightened invariants.
- Keep detection and classification in deterministic Go. Agents propose artifacts; the broker validates them.

## Non-goals

- Blocking or reverting drift on suspicion.
- Writing someone else's change into source automatically.
- Learning behavioural properties such as latency or logic bugs from drift labels; health signals and judgement cover those.

## Key constraint

Autonomous labels cannot discover latent faults. A latent fault has no impact by definition, so impact-based labelling would call it benign. Flagging a value as wrong needs either an invariant or a judgement on the specific change. Every invariant comes from a real event:

1. **Incident-derived:** reflection compiles the violated property after a verified incident.
2. **Drift-opened incident:** the responder explains an unexplained change, establishes a fault with evidence, and the fix is independently verified. Reflection then compiles the invariant from the captured drift.

Speculative invariant mining (agents proposing checks with no failure behind them, validated only against the healthy state) is rejected as brittle. A single healthy snapshot satisfies many accidental properties, and mutation tests show only that a check can fire, not that it matters.

## Components

### 1. Change feed (controller core)

- The deployer applies with server-side apply under the field manager `sdo-deployer`. The fields SDO's manifests set are the declared intent. Fields other managers own on accepted objects, such as HPA replicas or injected sidecars, are outside declared-field drift. Source-absent objects use the separate admission rule below.
- On controller startup or resume, and after an accepted source commit or verified SDO repair, the controller renders accepted manifests through the deployer's renderer with the deployed image and adopts their declared fields using server-side apply as `sdo-deployer` with force-conflicts. It records the adopted object count, taken-over field count, and prior managers once in durable controller status. A dry run is compared with live state before each write. Existing differences are recorded as unexplained drift and left in place for the normal classifier and responder path; adoption never silently repairs them. Repeating adoption over already-owned fields emits no drift.
- An informer watches the application namespaces. On each add, modify or delete it computes:
  - a field-level diff against the previous object;
  - a diff against the accepted source manifest, normalised through a server-side apply dry run (`DiffAgainstSource`). Because that dry run keeps other managers' fields, `Changed` also counts undeclared fields that were absent when SDO accepted the object. Once the object has settled (controllers have observed its spec; a Pod is scheduled), adoption records every path it carries (no values) in the `sdo.dev/accepted-paths` annotation, bound to the path shape of its accepted source. SDO writes the record in a separate metadata-only server-side apply under its own field manager, `sdo-acceptance`, so deployer applies (which never carry it) leave it in place. Server defaults, fields present at adoption and kubernetes.io/k8s.io labels, annotations and finalizers never count, and the record reaches corpus captures, so replay computes the same `Changed`. The record is also bound to the object's UID, so a record copied onto a recreated object is not current. When accepted source changes shape, the record is stale until adoption re-records it; re-recording keeps earlier additions counted. When an object SDO accepted before comes back without a record of its own (recreated, or the annotation stripped), adoption re-records only declared fields, deployer-owned fields and unowned server defaults; undeclared fields another manager owns stay additions. The controller keeps the set of accepted objects in its state across source commits. A failed record write affects only that object (typed error, adoption backoff), and adoption waiting for an unsettled object backs off from 5 s to 2 min. With declared fields equal and no current record, `DiffAgainstSource` fails closed with an error. `ReconcileFromSource` deletes the map entries the record marks as added. New values of undeclared fields that existed at acceptance are not reported. The Deployment controller copies the annotation onto ReplicaSets; records over 32 KiB are not written. Event admission still uses deployer-owned paths only;
  - attribution, by diffing `metadata.managedFields`: manager, operation and time.
- It emits a `DriftEvent{ref, fields, old, new, source, manager, observed_at}`.
- A second admission path covers **foreign objects**: live objects in the application namespace, absent from the accepted manifest set, of a declared effect-bearing kind, with no controlling owner reference. Initially the kind list is exactly `networking.k8s.io/v1/NetworkPolicy`. Controller-derived ReplicaSets, Pods and EndpointSlices do not enter through this path. Any `controller: true` owner reference excludes an object; noncontrolling references do not prove derivation.
- A foreign add carries `old=nil`, `source=nil`, and the complete current `spec` as one field, with source description `absent from accepted source`. Updates carry complete old/new specs; metadata/status-only updates are ignored. Foreign deletes do not open new incidents. They re-evaluate retained active program bindings even when the object has disappeared from namespace enumeration; every active binding needs an explicit `NoMatch` before its learned finding clears. Missing bindings or `Unknown` never prove repair. Initial informer-list objects remain excluded; learned periodic sweeps can still see them. Startup discovery of unexplained preexisting foreign objects is unbuilt.
- Absence must be established from a successful read of the accepted deployed manifest set (`ErrAcceptedManifestAbsent`, distinct from all other source errors). Missing revisions, renderer failures, wrong identities and unavailable source fail closed; they are never interpreted as absence. Manager names provide attribution, not admission authority.

### Admission decision (review requested)

Scores use one rubric: 1 = poor, 5 = strong; equal weight. Cost includes routine controller churn, legitimate operators and first investigation. These are design estimates, not live measurements.

| Alternative | Low false-positive volume / responder cost | Responder and invariant visibility / first catch | Generality across kinds | Small trusted surface | Migration | Total |
|---|---:|---:|---:|---:|---:|---:|
| A: namespace + effect-kind list + no controlling owner (chosen) | 4 | 5 | 4 | 5 | 5 | 23 |
| B: only objects selecting/referencing accepted workloads | 5 | 4 | 3 | 2 | 3 | 17 |
| C: no new admission; learned program sweeps only | 5 | 1 | 4 | 5 | 4 | 19 |

A adds zero ReplicaSet/Pod/EndpointSlice churn and no HPA or unknown-kind events. A standalone policy from another operator, including an unrelated or permissive policy, incurs one unexplained event per spec observation. Existing deduplication and budgets bound repeated work; admission is a question, never a bug verdict. Controlling owner references are a noise filter, not a security boundary: a malicious actor can spoof one. This controller is not an intrusion detector.

B suppresses unrelated policies but puts selector/reference semantics and accepted workload membership in the trusted classifier for each kind. Empty selectors, match expressions, future workloads, reference changes and temporarily missing pods complicate first-catch coverage. A exposes the namespace-scoped spec and lets evidence-grounded diagnosis determine relevance. Programs already see snapshot NetworkPolicies; the source-diff primitive must also support confirmed absence.

C catches repeats only after an invariant exists. With healthy readiness and no invariant there is no event to investigate, verified first repair, or evidence from which to learn. It cannot establish the first-catch claim.

Grow A's kind list only with a production impact argument, normal-churn tests, snapshot/read-view support and an evidence-grounded invariant example. Service selectors, PDB/HPA references, Ingress and ConfigMap relations are future candidates, not implicitly admitted. HPA and Ingress also need watches/view support. No benchmark names or oracle metadata participate.
The design does not depend on admission webhooks or audit logs. Either can add user identity later where the cluster allows it.

### 2. Classifier (deterministic, per event)

| Step | Condition | Result |
|---|---|---|
| 1 | Neither a changed declared field owned by `sdo-deployer` nor an admitted foreign spec change | ignore; not an unexplained event |
| 2 | An invariant program (Diagnoser) returns `Match` on an affected binding after the change | **fault**: open an incident with `latent` severity; the responder receives the match and selects any mitigation |
| 3 | A trusted expected-drift rule classifies it `Benign` | benign, logged |
| 4 | Otherwise | **unexplained**: open an ordinary `drift.unexplained` incident, or attach the observation to the open incident; this is a question, not a fault verdict |

The program sweep runs **on change** for the affected bindings, with a periodic full sweep as a fallback.

### 3. Unexplained drift handling

**D1 — full merge (accepted):** unexplained drift opens an ordinary latent `drift.unexplained` incident carrying the existing event and capture. The responder first asks “is this a fault?”, then diagnoses and repairs in one session. Learned matches retain `drift.<program>` and the trusted-program path. Delete the investigator task, Job and queue.

Go retains feed/admission, classifier, dedupe, batching and learned sweeps. Drift-only batches coalesce for 20 s / 8 events and are otherwise admitted like health findings; the per-hour dispatch and token budgets were removed (S1 PR 1). Charge responder usage once. Delete the investigator-specific execution/state path.

During an open incident, append drift to its evidence/findings instead of dispatching again. Extend existing incident-view, late-findings and `status`/`diagnose`/`verify` reads to expose it after launch; preserve Job-request idempotence. Existing locking and closure recheck must include late drift. No new generation protocol is needed.

**Evidence before action:** fault requires a concrete source contradiction AND a violated application relation or observed impact. Reuse evidence-kind validation, captured/tool observations, root-cause citations and repair receipts to establish evidence preceding mutation. Source absence alone, or missing containerPort declarations alone, is insufficient. Missing evidence means **unsure**, no action, unresolved. **Benign** means no action with a reason, not quiet health alone. The responder keeps write access: prompts enforce this ordering; existing diagnosis verification/attribution rejects unsupported repair credit and learning. It cannot prevent every direct kubectl write; D1 accepts that limitation.

**Verification/closure:** reuse drift source comparison and `recordDriftClears` in loop refresh/verify and controller closure. Require existing health clearance PLUS every retained drift finding, including late arrivals; registered detectors cannot replace synthetic findings. Apply the same checks to status, agent-requested verification and residual follow-ups.

- Unexplained drift clears on a fresh successful comparison proving the affected fields restored or source adopted, or a checked benign disposition. Source errors stay unresolved. Confirmed foreign deletion can clear the source difference; disappearance without attributed repair is externally cleared, not mitigation.
- Learned drift requires explicit `NoMatch` for **each retained active binding**, including deleted objects. `Unknown`, missing bindings, errors and timeouts are not clear; one binding cannot clear another. This is already fixed in `8ec987c83:recordDriftClears`; preserve it and integrate the loop/health-configured closure paths.

**D2 — exact benign labels (accepted):** add only repeated `DriftEvent` assessments to ordinary `IncidentResult`, reusing verdict/reason/evidence fields (`investigation_*` remains legacy wire spelling). Validate observations against controller-owned incident evidence. Persist checked labels in existing outcome `drift_events`; remove the verdict ConfigMap. A wholly benign no-op closes as `cleared_without_action`, with no mitigation/trust credit; mixed outcomes retain individual labels. Update the no-action result guard accordingly.

Reuse `driftEventKey` and current/source-value comparison, including manager, sorted fields and relevant source content; exclude timestamps, old values and unrelated outcome commits. Only benign suppresses exact observations; changed facts or contrary health/program evidence can dispatch. Read accepted outcomes through `memory.Reader` after restart/reset; seed rollback discards labels too. Never suppress fault/unsure: dedupe pending/open work only. Repair clears the finding normally, so an identical recurrence opens even without a learned program. No episode state machine is needed.

Capture directly in the ordinary incident session (`open@detect`; the settle and pre-apply captures were removed); late evidence uses existing capture references. Reflection learns from the cause's original capture and verified fixed/healthy cases. Reset drains or cancels/awaits workers **before** deleting sessions/checkouts. Ordinary terminal `ResponderJobFailedError` handling replaces investigator retries until the 1 h TTL; unresolved failure is never healthy closure.

### 4. Autonomous labeller (deterministic)

It resolves unexplained events from observable evidence only:

| Signal | Label |
|---|---|
| Health or SLO detectors fire on the object or its dependents within window W | bad |
| Persists for W with all checks quiet | weakly benign |
| A later accepted source commit adopts the value | strongly benign |
| A later deploy reverts it, and health then degrades | needed |

Each label, with captured before and after snapshots, is stored as a drift corpus case at `.sdo/corpus/drift-<id>/`. Its provenance comes from controller captures, as for incident cases.

### 5. Learning (reflection and maintenance pass)

- **Fault drift with an independently verified fix and no covering program:** compile an invariant program through `program scaffold` and `program propose`, with the drift case as its open example.
- **Benign drift that fired an invariant:** add a NoMatch case and tighten the invariant. Repeats lower its trust.
- **Repeated benign drift of one shape:** compile an expected-drift rule (see the SDK section). The trust threshold is K = 3 strongly or weakly benign cases with no bad case.

### 6. Write-back (later phase)

- **SDO's own repairs:** after verification, propose the equivalent source change, validated by the broker. That way SDO-created drift converges.
- **Frequent drift on one field:** propose a structural source change, such as dropping `replicas` when an HPA manages it.

## SDK and contracts

- Retain `DriftEvent` and drift corpus cases; reuse their evidence in the ordinary incident path.
- `DriftRule` is a new interface, with the same validation as programs (corpus replay and rename invariance):

  ```go
  type DriftRule interface {
      Spec() ProgramSpec
      Classify(ctx context.Context, v ReadView, e DriftEvent) (DriftVerdict, error) // Benign | Unknown
  }
  ```
- `ReadView.DiffAgainstSource` compares accepted objects using the server-side apply dry run. For a confirmed source-absent live object it returns `Diff{Changed:true, SourceAbsent:true, Text:<accepted:null and observed spec>}` without an apply or dry run. If both source and live are absent it returns unchanged with `SourceAbsent:true`. Snapshot NetworkPolicies expose complete selectors, policy types and rules without requiring an accepted policy. `SourceAbsent` is an additive Go read-view field, independent of worker identity. Wrong source identities remain errors.
- Source absence does not authorize deletion or reconciliation. Existing repair policy and verification still apply. A diagnosis-only invariant can catch a repeated deny policy while leaving mitigation to the responder.
- Future expected-drift rules belong to the responder-owned playbook-memory class; no sixth artifact class is added.
- Outcome and `sdo status` include drift events and their classification.

## Ownership

| Part | Owner |
|---|---|
| Change feed, classifier, labeller | Controller core (deterministic Go, no LLM) |
| Drift assessment, repair and benign reason | Ordinary responder; Go verifies, broker records outcome |
| Invariant programs, drift rules | Proposed by the responder or reflection; validated by the broker |
| Health detectors | Health judge (unchanged) |
| Source write-back commits | Broker, under the `commit` policy |

## Implementation status

| Part | Status |
|---|---|
| SSA adoption, cache feed, declared-field classifier, on-change and periodic sweep | Built |
| Separate investigation dispatch, queue and verdict ConfigMap | Removed by D1; no runtime compatibility path |
| Ordinary drift incidents, benign result labels, verification/closure integration | Built; ordinary responder tasks retain late drift and closure checks every active binding |
| Per-binding explicit NoMatch / Unknown-is-not-clear | Built in `recordDriftClears`; integrated into loop and health-configured closure |
| Foreign NetworkPolicy admission and source-absent read-view diff | Built; hermetic admission, source-failure, deletion-clear and churn checks pass |
| Learned invariant proposal/validation and deterministic registry execution | Built; captured NetworkPolicy repair passes proposal materialization, corpus/rename validation and compiled repeat detection |
| Autonomous labeller and confirmed expected-drift rules | Unbuilt |
| Startup first-catch discovery of preexisting foreign objects | Unbuilt; initial informer adds suppressed |
| Source write-back and structural proposals | Unbuilt |
| Additional foreign kinds and relation-scoped admission | Unbuilt; explicit extensions require churn evidence |

The deployed drift flag is `--drift-batch-window`. Removed investigator config tags 59–61, state tag 32, the drift budget config tags 67–68 and state tag 33 are reserved; coordinated controller/worker images use the ordinary IncidentResult assessments. Synthetic detector IDs use dots to obey the mainline grammar. New run metadata declares `token_accounting_version: responder-drift/v2`; `drift_tokens` is legacy zero and merged reasoning is charged once to responder usage. Historical costs remain readable.

### Hermetic coverage and churn

At `8ec987c83`, `TestHermeticForeignNetworkPolicyOpensLatentIncident` exercises the actual cache handler, classifier, ordinary responder admission, latent incident and deletion clearance. `TestHermeticForeignNetworkPolicyInvariantRepeat` uses its real open/fixed captures, existing primitive recordings, typed proposal assembly/materialization and builder corpus/rename validation. It executes the compiled `networkpolicy-service-isolation` artifact through the registry on a second policy name and opposite direction, opening `drift.networkpolicy-service-isolation` without a responder classification session. The repeat closes after deletion and explicit `NoMatch`, while a failed source read returning `Unknown` keeps it open. These checks establish hermetic plumbing and scripted learning, not live connectivity or mitigation-oracle results.

The scripted invariant is diagnosis-only: a source-absent policy fully isolates a pod selected by an unchanged accepted Service with declared ports. It checks ingress and egress separately and conservatively suppresses matches when another selecting policy has allow rules. Partial rules require judgement. The scripted reflector can propose this class from a removed policy and a retained healthy policy peer (the existing same-kind corpus requirement; an application with no remaining policy peer needs a separate deletion-corpus abstraction); targetPort proposal behavior remains available. These existing fixtures do not establish autonomous live policy mitigation.

`TestForeignDriftNormalChurn` covers every supported informer kind. HPA is not an informer kind; `TestDriftClassifierIgnoresHPAOwnedReplicas` separately covers its Deployment writes.

| Kind | Feed | New events in controller-owned/unowned normal-churn snapshot | Foreign standalone admission |
|---|---|---:|---|
| ConfigMap | Automatic drift watch | 0 | No |
| Service | Automatic drift watch | 0 | No |
| Pod | Automatic drift watch | 0 | No |
| Deployment | Automatic drift watch | 0 | No |
| ReplicaSet | Automatic drift watch | 0 | No |
| Endpoints | Automatic drift watch | 0 | No |
| EndpointSlice | Automatic drift watch | 0 | No |
| NetworkPolicy | Automatic drift watch | 0 (controlling owner) | Yes, including unrelated/permissive policies |
| Event | Only when declared by a detector | 0 | No |

Unchanged declared objects still produce no events. Changed accepted `sdo-deployer` fields retain their existing path. Source-absent kinds outside the list remain ignored even if they retain stale deployer ownership. Ordinary standalone policy churn can still cost responder sessions; the zero above applies to controller-derived objects, not all policy changes in a shared namespace.

## Decision, removal and implementation

One rubric for all options; costs are estimates, R = responder, I = investigator, F = reflection when needed.

| Alternative | Defects removed | Tokens: latent / health incident | False-action risk | Deleted versus added | Migration |
|---|---|---|---|---|---|
| A: full merge (chosen) | Remove split paths (1)/(9)/(10), fix (2), preserve (13) | R+F / R+F; attached evidence adds some reasoning | Existing write risk; evidence and verification limit it | Delete worker/protocol/queue/store; small routing/label/verification edits | Drain; matching worker/controller release |
| B: cheap responder triage before write credentials unlock | Same fixes as A | R+F plus triage / health evidence skips triage | Lower if shell credentials are really gated | A plus credential/tool gate and recovery | Additional RBAC/session changes |
| C: keep investigator, fix four defects | Patch (1)/(2)/(9)/(10), preserve (13) | I+R+F / R+F plus duplicate I | Read-only triage helps; later writes remain | Retain dual protocols/jobs/state, add fixes | Least capability churn; continued dual-state maintenance |

Choose A: one session reuses evidence and removes the independent queue/lifetime. Dispatch alone does not fix verification. B's credential machinery is outside this change; C retains the duplicated reasoning seen in the audit.

| Audit | Resolution |
|---|---|
| (1) cached bug suppresses repeat without program | Cache benign only; repair clears normally, identical fault reopens without learning. |
| (2) verify/closure drops drift | Add retained unexplained/learned findings to existing verification/closure, even with healthy health detectors. |
| (9) reset deletes referenced capture | Remove investigator lifetime; drain or cancel/await ordinary workers before cleanup. |
| (10) failed investigator Job blocks until TTL | Delete queue/Job path; ordinary terminal failure handling retains unresolved status. |
| (13) per-binding clear; Unknown not clear | Already fixed in `recordDriftClears`; preserve and cover loop/health-configured closure. |

Implement on the completed `vic-lsh/feat/drift-on-main` merge. Runtime files retain their names: `sdo/operator/runtime/{drift.go,controller.go,responder_loop.go,registered_reflection.go,run.go}`. This snapshot's `operatorcli/draft.go` (`draft-test`, `proposal validate`) meets main's `operatorcli/memory.go` (`memory check`), `memory/layout/` and `memory/validate/`. Preserve `memory/corpus_capture.go`, logical IDs/ProtoJSON (#15/#16), and `MemoryDiagnostic` retries in `runtime/native_broker.go` / `agents/memory_diagnostics.py` (#19). Apply the sibling `docs/design/memory-edits.md` contract without coupling dispatch to typed proposals versus isolated-checkout edits; Go/broker validation remains authoritative.

Delete `runtime/{drift_investigation,registered_drift}.go`, completion/state/wakeup wiring, `agents/responder/drift_worker.py` and prompt, investigator registrations/RBAC, verdict ConfigMap/budget path, and `scripted_drift_investigation`/CLI mode. Remove `DriftInvestigationTask`, `DriftInvestigationResult`, `DriftEventInvestigation` from `worker.proto`, generated exports and fixtures. Reserve removed config tags/names; retain historical event readability. Drain/cancel before cutover, discard old suppression cache and coordinate protobuf schema changes with matching images. The v1alpha1 schema may evolve freely; reserved field numbers prevent old stored state from being misread. No migration engine or dual runtime.

**D3 — accounting (accepted):** retain fastloop `drift_tokens` as legacy zero for new runs; charge merged reasoning once to `responder_tokens`. Old runs keep recorded investigator costs. Document accounting version in run metadata.

Three failing-test-first steps, **2.75 h total**, extracting only a small shared drift-check helper:

| Step | Estimate | Change and hermetic exit |
|---|---:|---|
| 1 | 1 h | Add failing-first `TestRepairedDriftRecursWithoutProgram` (1), `TestVerifyRetainsDriftWithHealthyDetectors` and `TestClosureRetainsDriftWithHealthyDetectors` (2). Route/attach drift through existing incidents and reads; delete investigator code/contracts. Rewrite `TestInvestigatedTargetPortOpensLatentIncidentWithDriftCorpus` and `TestInvestigatedDriftDuringResponseOpensAfterAcknowledgment` for one-session handling and original capture. |
| 2 | 1 h | Reuse drift checks in verify/closure and existing attribution; add benign result labels. Pass recurrence with failed/no-change reflection and restart. Rewrite `TestHermeticForeignNetworkPolicy*`, scripted worker and `tests/unit/benchmarks/sregym/adapter/test_scripted_drift.py`; cover first catch/compiled repeat, two bindings, deletion, Unknown/source error, quiet health and benign no-op. |
| 3 | 0.75 h | Order existing reset drain/cancel before deletion; test terminal failure without TTL wait. Rewrite `TestHermeticDriftVerdictAcrossFastloopResetAndNextIncident` for exact benign reuse, changed/new observations and no fault suppression. Update cost readers, architecture's three matrices and analysis references; run focused Go/Python tests, `scripts/test_fastloop_hermetic.sh`, format/error checks. |

Open questions and recommendations: **D1**, retain write access? **Yes**, with evidence-before-action and verified credit. **D2**, where persist benign labels? **Existing outcomes**, exact observations only. **D3**, remove `drift_tokens`? **No**, retain legacy zero and document accounting.

Optional follow-ups only: credential gating; generic relation-verification DSL; new capture/episode/generation or migration machinery; broad benign rules/labeller; startup foreign discovery; more foreign kinds; live cost/catch→repair→learn→repeat validation. None is required for this merge.

## Evaluation

- **Streams:** add legitimate-drift events (HPA scale, a log-level env toggle, a canary image change) and app-change commits between injected faults. No human acts in any stream.
- **Metrics:**
  - detection latency for latent faults;
  - false-positive incidents on legitimate drift over time;
  - learned invariants and drift rules, with precision and recall against injected faults;
  - LLM tokens per event.
- **Arms:**
  - SDO with unexplained-drift incidents and learning;
  - SDO with health-only admission and learned sweeps, to isolate first-catch admission;
  - the baseline, given the same source checkout and audit framing;
  - the baseline, also given the same change notifications, to isolate what learning adds beyond the notification.
- **Scoring:** report latent and user-impacting faults separately.

## Risks

- **Responder false actions.** Evidence-before-action, no-action default and independent verification limit risk; direct write credentials remain a limitation (D1).
- **Responder cost under high drift volume.** Mitigated by ownership filtering, exact benign labels, learned rules, one incident session, deduplication, batching and the budget.
- **Field-manager quality.** Generic or spoofed manager names weaken attribution. The labeller's impact and source-adoption signals do not depend on them.
- **The labeller's window W** trades detection speed against label quality. Start at 10 minutes and measure.
- **Benchmark artifacts.** SREGym injects by mutating live state, which suits drift detection unusually well. Add source-borne fault variants, where the bad change is committed to the app repository, so that results do not rest on drift alone.
