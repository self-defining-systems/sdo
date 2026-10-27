# SDO architecture

The deterministic SDK supplies shared snapshot candidate detectors in `sdk/observe` and observed-listener target-port diagnosis and plan proposals in `sdk/routing`. Applications import these helpers instead of copying them into per-app `.sdo/lib/`; app policy, provenance, bindings and app-specific helpers remain in operational memory. This does not share agent-learned code across repositories or change the memory layout. Promotion of the `repair` helper is deferred until XV1 settles its action vocabulary.

This document describes the production boundaries implemented in this repository and the separate SREGym benchmark boundary. It does not treat historical deployment experiments as production features.

## End-to-end lifecycle

```text
human health objective
        |
        v
agent proposal in isolated checkout -> trusted source build/apply/readiness -> broker commit
        |
        v
source-grounded architecture proposal -> broker commit
        |
        v
fresh health-judge proposal -> isolated detector validation -> broker commit
        |
        v
controller builder/install -> deterministic Go controller -> incident batch
        |
        v
incident-open coverage -> ordered response stages -> independent verification -> broker outcome commit
        |
        v
fresh reflection proposal -> broker validation/commit -> controller rollout
```

`sdo-operator host` starts the active production lifecycle; the installed `sdo-operator operate` continues it in the cluster. Go coordinates separate Human goal, Upkeep schema, Deployer source and architecture, and HealthJudge bootstrap proposals. The source stage requires a trusted deployment receipt bound to the exact candidate Git tree and immutable running images before publishing; architecture cites the broker-returned accepted source commit. The controller then runs deterministic detectors and independently verifies recovery. At incident open, Go builds finding-bound playbook coverage and runs configured `ResponseStage` implementations. SDO applies a learned playbook automatically when its diagnose matches live and every parameter binds (stage [deterministic, agent]); the agent is told the result and handles what remains; no automatic rollback, no trust gate. The default `deterministic,agent` pipeline first applies accepted playbooks with a fresh Match, one mitigation entry point and complete finding bindings; it waits at most 30 seconds for its own rollouts and retains completed writes. The registered responder receives remaining findings and typed `prior_stages` reports, including receipts, post-apply diagnoses and skip reasons, and chooses further repairs through the menu. `response_stages: [agent]` selects the baseline. A stage never closes an incident; independent controller verification does. Incidents cleared without a responder still receive fresh reflection with stage evidence. Go independently verifies recovery; only the responder decides rollback. Verified program success/failure counts are information, never write authority. Before recording each successful outcome, Go settles learning by confirmed cause. Exact covered repairs and fixed-capture false matches get deterministic corpus cases through normal validation; questions or rejected cases go to in-session reflection. Trust consumes only the persisted covered instances. A full host-to-controller kind smoke passed with live clear detector evaluations; deployment still depends on the target cluster, registry, credentials, and application manifests.

## Component boundaries

Go owns operational decisions and accepted repository state. Python hosts the built-in agent implementations. Registration is implementation neutral: a versioned ProtoJSON `AgentRegistration` advertises an agent ID, capabilities with exact protobuf request/result type URLs, and a launch method. `AgentTask` and `AgentResult` carry typed protobuf `Any` payloads and a proposal workspace. The Go registry rejects unknown IDs, capabilities, and payload types; the generic Kubernetes executor launches a registered worker without knowing its language or model provider. Built-in manifests live in `sdo/agents/registrations/` and are the only source of agent IDs and task types: the controller loads them from a configured directory (`--agent-registration-directory` or `ControllerConfig.agent_registration_directory`, image default `worker_registry.DefaultDirectory`) and resolves tasks by message type URL; third-party workers can use another language if they honor the same contract. Incident, lifecycle, and reflection dispatch use this boundary. Go also owns validation retry: when the broker rejects a reflection or health-judge proposal with validator diagnostics, Go re-dispatches the task as a new task carrying the typed `MemoryDiagnostic` list and a 1-based `validation_attempt` (`ReflectionRequest`, `HealthJudgeRequest`), at most `broker.MaxValidationRetries` (5) times. Every rejection is recorded durably before the checkout is reset (`BrokerLedger.reflection_rejections` for reflection, a `ProposalRejectionLog` beside the broker workspace for the health judge), so a restart neither resets nor repeats attempts; once the bound is spent the rejection is terminal (`BrokerLedger.reflection_rejected`, and the health judge falls back to the whole-deployment retry). Workers only render the diagnostics into the prompt. A lifecycle worker (deploy, architecture, health judge) only authors a proposal in an isolated checkout, so one whose Job vanished with a stale dispatch receipt is discarded by the registry (`DiscardVanished`, refused for other task types and while a Pod of the Job still runs) and dispatched once more. For the health judge a repeat is a recorded `worker.job_vanished` attempt on a fresh task, and a rejection log made terminal only by such attempts does not pin later controller starts: the refresh takes the next publication generation (`-gN`). Deploy and architecture failures record feedback text and are bounded by the install attempts. Incident and reflection tasks keep the terminal `JobVanishedError`, which the runtime records as a failed task.

The `.proto` files are the source of truth for messages crossing Go, Python, and worker boundaries. The controller retains SDK types for live scheduling and detector execution, and exports/restores the generated `ControllerState` directly; there is no duplicate persistence struct. Dispatch and closure phases use generated enums. Request, result, and closure projections remain explicit at SDK boundaries. Outcome derivation constructs generated outcome messages directly. The built-in Python workers parse and build the generated messages directly; model-output JSON schemas are generated from the proto descriptors (`sdo/contracts/model_schema.py`) for `IncidentResult` and `ReflectionResult`; the reflection decision file Go loads is that same `ReflectionResult`. The responder and its resumed reflection turn send one session wrapper (`sdo/agents/responder/session_turn.py`): a strict object with nullable `incident_result` and `reflection` branches, plus one shared MCP server set, so the resumed turn keeps the responder's prompt-cache prefix. Each turn fills only its own branch; a wrong or null branch gets one resumed correction before it fails. The controller's persisted state (the runtime-state ConfigMap) and the broker ledger are canonical ProtoJSON of `ControllerState` and `BrokerLedger`; the Go structs in `runtime/protocol.go` carry no JSON tags and convert through `runtime/contract_projection.go`. Closure and reflection decisions use pure generated-state transitions in `runtime/lifecyclecore`. Their private I/O adapters live in `runtime/internal/lifecycleio`; only the driver persists transitions and executes effects. Failed closures use a content-addressed ConfigMap archive outbox before later effects are authorized, and registered reflection requests are durably frozen outside proposal checkouts. See [the lifecycle specification](lifecycle-state-machine.md) for states, events, invariants and the functional-core extraction plan. The incident view published for Python and benchmark readers is a ProtoJSON `IncidentView`. Canonical ProtoJSON omits empty fields, so readers tolerate absent keys. Reflection consumes generated outcome messages directly. Dynamic finding metadata is the only field in the Go incident projection that needs a JSON value conversion. Conformance tests cover the projections and reject contract drift.

Controllers may share one accepted application checkout while keeping separate proposal, session/corpus and broker-ledger roots. The existing application-root/repository-mount configuration selects that checkout; sharing across hosts requires a shared filesystem that honors the broker's file lock. The broker lock lives in the accepted repository's Git common directory, so separate controller workspaces serialize the full validation and accept transaction. Stale diffs are three-way replayed onto the current accepted head; the complete `validate.Check` and independent compiler validate that merged tree. Text conflicts and merged-tree validation failures return typed `MemoryConflict` provenance through `MemoryDiagnostic.memory_conflict`, including affected paths, accepted commits/incidents and validator errors. Reflection resumes the same provider session in a fresh current accepted checkout and merges both intents; after a conflict, at most three total publication/correction attempts are allowed. Verified deployment-source publication still requires its exact deployed base/tree. Incident IDs must be unique across sharing controllers. Live eval lane mounts, cross-cluster storage and runner orchestration remain benchmark concerns.

Catalog reads for incident and reflection tasks use `memory.SnapshotAt` with the prepared proposal's accepted base. The Git storage edge projects raw immutable Git blobs without checkout filters or a shared index, so peer publications cannot mix revisions or substitute newer learning into an already frozen task. Ordinary latest-head reads retain their clean-checkout checks.

Native incident and reflection tasks carry an accepted-commit-bound `OperationalMemorySnapshot` with goal and architecture text, logical detector registrations, and playbook IDs/content. Reflection edits final operational-memory files in its isolated checkout and returns decisions and logical playbook IDs. The Go broker derives the actual diff and calls the same `validate.Check` as the local CLI before publishing. During response, playbooks and shared libraries are editable working code. Agent program commands compile changed dependency closures on the controller with its warmed Go cache. An isolated Go program forwards read-only view calls and returns typed plans to the ordinary controller executor, preserving bindings, receipts and independent verification. Program revisions use `working-tree:<source digest>` until publication. Source publication projects only source repairs; working playbook changes remain in the incident checkout and seed the resumed reflection checkout exactly once. Reflection submits those drafts through the complete memory check and broker validator. The health judge uses the versioned `health-judgment-direct-v1` capability to edit final health detector, traffic and health-objective playbook files directly. It returns metadata and logical detector IDs and rejects retired proposal payloads. Dispatch pins `SDO_ACTOR=health_judge`, `SDO_PHASE=initial`, and the accepted base. The explicit `memory.PrepareHealth` step in `validate.Check` installs generic service-endpoint and traffic detectors from the final workloads, then the same validation and compile pipeline checks the prepared tree. The broker publishes all prepared files; independent health verification remains separate. The judge can refine health diagnostics after bootstrap while source, goal, outcomes, incident detectors and other playbooks remain protected.

The controller captures the application namespace's detection view at detection (`open@detect`), immediately before the first live repair (`open@pre-apply`), and at verified close (`fixed`) under the private incident session; with no pre-incident healthy reference, the controller binds the `fixed` capture as the incident's healthy sample for detector checks. The `open@detect` snapshot is written before the responder is dispatched; its routing sweep runs in the background and never delays dispatch, and the worker requests a controller-owned pre-apply capture before `sdo act` executes locally. Typed plan application uses the same first-mutation boundary. Pre-apply reads a fresh provider snapshot, preserves detect unchanged, and adds no probes or settle delay; the protected file is reused across repeated commands and restarts. Capture failure records an evidence gap and allows the repair and its receipt to proceed; controller-observed failures retain `pre_apply_capture_gap` across restored responder state, so later commands cannot substitute a repaired snapshot for the missed boundary. For finding Services, Services the snapshot shows as unroutable (no ready selected Pod, or an integer targetPort no selected Pod declares), and up to two ready healthy peers in that priority order, the controller also records real TCP observations at Service ports/targets and at selected Pods’ declared ports and numeric targets, using captured Pod IPs through the credential-free isolated prober. Automatic sampling is bounded to 24 probes per capture, two Pods per Service, four backend ports per Pod, 4 s per probe and six seconds total. Fixed snapshots refresh these reads. Declarations choose candidate probes; they do not prove a listener. Captures retain ConfigMap `data`/`binaryData` key names with empty values and
`sdo.dev/configmap-values-redacted: "true"`; live detector reads retain their
original values. Previously accepted immutable corpus cases are not rewritten.
Captures omit Kubernetes write metadata and sensitive environment values. Reflection builds fixtures from the incident snapshot, recorded actions and available primitive observations; controller capture files and digests are optional. When the incident closed verified healthy, each cause a recorded repair touched requires a diagnose program, mitigation program, handling README and detector coverage, or a typed SDK gap naming its resources. The shared Go validator rejects missing coverage with correction diagnostics. Accepted cases remain immutable. Programs compile and must execute mitigation to clear diagnosis in replay; accepted manifests are explicit fixture inputs for source-based repairs. A broker-validated new program receives one origin credit tied to its independently verified cause and bound replay fixture; its first recurrence exposes that verified origin count to the responder with the matching program.


RunScript is a first-class learnable mitigation alongside typed actions. Mitigate
returns inline bash and relation-derived Values resolved into process environment.
A fixed tool image executes an application-local Job with `backoffLimit: 0` under
`sdo-script`, which holds the same cluster grant as every SDO identity. The installer accepts `--script-image` for the
slim `script-tools` build target; existing installations reuse the cached trusted
controller image. Script execution defaults to 60 seconds, bounded by five minutes
and the remaining incident budget, including a short evidence-publication reserve.
Scripts and each retained output stream are bounded at 64 KiB.

An application-local private ConfigMap reserves the application before Job creation
and records the Job UID. A restart joins that Job or recovers its result; an
uncertain missing/replaced Job never permits another invocation. The native runner
routes kubectl through the existing Kubernetes proxy, checkpoints first-write
preimages before forwarding and records settled postimages, receipts and explicit
capture gaps. Object images use private chunked ConfigMaps so a large object or repair batch
cannot exhaust the bounded result envelope. Raw output/preimages stay at that runtime storage edge; durable
outcomes retain references, digest, exit code and input provenance. Arbitrary
external effects remain opaque. Undo is explicit only; script undo from captured
preimages is follow-up work.

Script corpus labels link the authored SHA-256 digest to the original successful
timed repair receipt. The memory gate accepts `observed-transition`: open Diagnose
must Match, fixed Diagnose must return NoMatch and bindings/inputs must validate.
It never supplies fixed captures to the action simulator or claims the rewritten
script ran. Live reuse goes through the ordinary apply→Diagnose and targeted
finding-clearance loop, while global health continues to own closure. Final
settlement records `final_verification_basis: observed-transition` after a fresh
bound diagnosis and scoped health sample; it never claims exact typed-effect
equivalence for a script.

| Package | Responsibility |
|---|---|
| `sdo/agents/{deployer,health_judge,responder}/` | Built-in agent sessions, prompts, local provider adapters, worker entry points, and typed task/result contracts; reflection and the judge author final files in their private checkouts |
| `sdo/agents/lifecycle/` | Shared source architecture and health judge agent prompts and DTOs |
| `sdo/contracts/proto/sdodev/contracts/v1alpha1/`, `sdo/contracts/`, `sdo/operator/contracts/` | Shared protobuf schema and generated Python/Go messages |
| `sdo/operator/worker_registry/` | Strict registration, capability lookup, and implementation-neutral Kubernetes execution |
| `sdo/operator/onboarding/` | Immutable, accepted-commit-bound namespace identity acknowledgement staged at native install/adoption and verified by the first confirmed complete in-cluster evaluation, with immutable never-served reachability evidence and durable sampling/rejection markers; no Secret reads or incident-time recapture |
| `sdo/operator/lifecycle/`, `deployment/`, `artifacts/` | Go lifecycle decisions, trusted deployment/readiness strategies, and physical bootstrap-artifact writes |
| `sdo/operator/rendered/` | Production deployment renderer and selected-manifest reader; no benchmark dependencies. |
| `sdo/operator/broker/`, `memory/` (including `memory/layout` and `memory/validate`), `outcome_derivation/` | Go commit authority (including repository-wide serialization, validated stale-diff replay, source-only repair projection, working playbook carry into reflection and append-only reflection SDK-gap annotations), logical snapshots and explicit generic-health preparation, the single operational-memory validator, outcome store with physical JSON adapter, controller-derived three-class outcomes without diagnosis verification, and read-only controller observations |
| `sdo/operator/{sdk,core,runtime,builder,install}/` | Detector and program SDK with canonical object-ref text parsing, deterministic execution, drift feed to responders, namespace-scoped verified plans, controller state and its typed projection, builder/supervisor, Kubernetes installation, and controller-only named write grants derived from accepted cluster source |
| `sdo/operator/runtime/{plan_script.go,script_job.go}`, `operatorcli/script_{runner,capture}.go` | App-local RunScript Jobs, durable application/Job identity, existing Kubernetes proxy recording with private first-write preimages and settled postimages; selected scope remains separate from measured writes |
| `benchmarks/sregym/` | First-party benchmark adapter, Go receipt checker and submission guard, versioned fault descriptions, suite capability preflight, runner, experiments, and analysis |
| `third_party/sregym/` | External benchmark harness Git submodule |

Production packages do not import SREGym code. Benchmark-only image stages live in `benchmarks/sregym/Dockerfile.worker`, extending the built production responder; development mounts expose only production sources, examples and the operator binary. Benchmark adapters may call production APIs; strict receipts and submission relays remain in the benchmark boundary. The native path passed an end-to-end kind smoke with real goal/source/architecture/judge artifacts, concrete deployment and health evidence, provider credentials, and controller installation. Detector SDK staleness/dependency interface changes are outside this refactor.

Object-reference identity on the wire is `common.proto:ObjectRef`. The SDK
keeps a comparable value projection for detector maps and plans. Its
`ParseObjectRef`/`FormatObjectRef` pair owns agent text parsing and canonical
formatting (including kubectl aliases and explicit namespaces). The Python
responder CLI forwards reference text verbatim; Go decodes it before handling
tools. Root-cause evidence entries are recorded as the responder cites them: the
generated schema checks their kinds, and neither the responder nor the
controller re-checks or retries an evidence source.

Native production receipts use `benchmark_receipt.proto` and the benchmark-only
Go module `benchmarks/sregym/protocol/native_receipt_go`. Its
`sdo-native-receipt build|validate` CLI decodes generated messages, derives the
flat summary, and checks broker correlation, health, repair, reflection, rollout
and Job evidence. A rejected receipt is a `ReceiptFailure` in a
`NativeReceiptResult`, with process exit status zero. Python forwards bytes and
persists diagnostics; `drain_pending_incident` returns a `ReceiptDrainResult`
for receipt rejection, and persistent/fastloop callers continue the stream.
Closure failures and drain scheduling remain separate controller-state concerns.

The host uses `SDO_NATIVE_RECEIPT_BIN` (absolute path), an installed
`sdo-native-receipt`, or builds the checker once into a process-lifetime temporary
directory from the local module. Source mode requires Go. Benchmark workers do
not need this host-side checker. The checker imports the public generated
contracts only; production Go packages do not import the benchmark module.

The benchmark submission bridge transports conductor status and acknowledgments
to `protocol/submission_go`. `benchmark_submission.proto` defines those facts,
phases, terminal-stage spellings, and the Go guard's typed decisions. The guard
authorizes a POST only for the observed matching phase and never authorizes one
during a completion poll. Python performs HTTP and sleeps; it does not interpret
stage order or acknowledgment status. Opaque upstream reply metadata is retained.
The benchmark worker copies only the separately built `sdo-submission-guard`
binary; production images contain no benchmark guard. Hosts may select
`SDO_SUBMISSION_GUARD_BIN`, an installed binary, or build once from source.
The upstream untyped `/submit` still has a status-read/POST race: phase checking
cannot make those separate upstream operations atomic.

Controller-state readers use `sdo-operator observe-controller` and
`controller_observation.proto`. The transport-neutral Go projection interprets
incident identity, verified closure, acknowledgment and permanent
closure failure using R1's generated `DispatchState`/`ClosureState` values.
Python polls and consumes the typed observation; it does not compare lifecycle
strings or rebuild these decisions from dictionaries. The endpoint opens no
Kubernetes client and never writes state. Controller publication validation
remains at the controller's storage edge; this read projection strictly decodes
field types and rejects undefined lifecycle enums. Source-mode operator launches
use a cached build, so observation polling does not repeatedly use `go run`.

## Mitigation menu

The menu is a convenience over direct commands. `sdo menu` lists accepted Mitigator programs with their
declared object and value parameters and registered raw plan actions as generic one-action plans.
`sdo mitigate OPTION [--NAME VALUE ...] [--rounds N]` runs the selected option through the ordinary
`PlanExecutor` under the controller's existing RBAC and always-on executor checks. The option catalog
comes from program and action registries, so adding an action requires no menu-specific execution case.
The menu adds no selection ledger, fences, quiescence, provenance, health-reader or credential-delta
preconditions. Direct commands and recorded `act` remain available in the same responder session.

Current source-absent `drift.unexplained` findings also expose a `remove-unexplained` option bound to the exact UID/resourceVersion. At the API delete boundary Go rechecks the current accepted-source inventory, onboarding acknowledgements, SDO ownership and control namespace. The controller executes and audits the conditional delete; the installer assigns discovered non-secret get/delete grants to the non-workload user `sdo-unexplained-removal:<control-namespace>`. Only the checked callback uses a separate transport impersonating that identity; generic menu/program actions retain the controller identity and its original grants. Controller impersonation is restricted to that exact user; no removal ServiceAccount or token is created. Generic object, raw API and exec clients, including diagnosis read views and observations, reject control-namespace access so plans cannot create controller-identity workloads or acquire controller credentials. Added removal authority is disabled when application and control namespaces coincide. Worker RBAC is unchanged. Cluster bindings require exclusively application ServiceAccount subjects. Cluster roles require every referencing binding to be removable and exclude bootstrap, built-in, system and exported aggregate roles. A separate complete RBAC ownership graph includes foreign RoleBindings; live ownership reads repeat the check before conditional deletion. Shared authorization receives a `shared_ownership` removal blocker in the menu and is never offered for removal. Direct command receipts retain only the explicit patch/delete targets, and deletion receipts require absence readback, and failed actions retain typed reasons. Source-absent inventory objects are drift information: they are listed in `drift[]` and recorded on the outcome, never held against verification or closure, so they cannot stop a response early. The all-denied stop remains `authorization_denied_no_progress`. The menu lists typed `unexplained_removal_blockers` for current findings that cannot be offered. Typed launch initializes the ordinary exec/dynamic transports even after constructing its own Kubernetes client; only explicitly caller-injected clients skip ambient credential setup. Go persists its terminal findings; health windows and pending rollouts retain the verification grace. Raw DELETE resolves its named whole-object path through discovery, rejects conflicting references, and requires confirmed absence; pending or unavailable readback reports `delete_not_confirmed`. Tool output separates five mitigation rounds from the provider continuation budget; final response-loop accounting counts provider executions. Supplied Kubernetes clients retain authenticated generic API transport and unsupported typed object kinds resolve through discovery. Unavailable API/exec actions are omitted from the menu.

Versioned generated menu/report contracts carry session, attempt, round and action identities.
The tool response includes parameter schemas, what ran, per-action results, each value's provenance,
and any error. Failed and successful reports return to the same responder session so it can choose
another option or fix the problem directly. The incident allowance is at most five rounds, shared
across options and ordinary plan batches; a smaller configured allowance remains effective.
The automatic response stage sends all its matched plans in one apply with one rollout wait, inside a
fixed 45 s stage budget (`DefaultStageBudget`); that apply is numbered separately and never spends the
agent's rounds. Matches the stage could not apply or verify in time reach the agent in `prior_stages`
with skip reason `stage_budget`.
The stage runs again, on the responder's own loop, when a finding that a learned playbook explains
activates after dispatch (`attachLearnedActivation`, at most eight passes, each within the same budget).
The finding joins the open incident, a pass redoes attempts that were only skipped and never repeats one
that wrote, and the one `deterministic` report in `prior_stages` is updated in place. The responder reads
the new attempts once, as `prior_stages` in its next tool reply.
Evidence for source-reconcile learning: every resource a write names, at each write boundary and at
detect, gets one `DiffAgainstSource` recording in the incident capture (`recordSourceComparisons`); an
object with no acceptance record keeps its declared-field result and the typed reason
`acceptance_record_unavailable`.
Private session/checkpoint messages retain provider identity for presentation, without mutation authority.
Session presentation, reports, repair receipts and counters survive through the existing tool-state
snapshot. There is no selection journal, report outbox or menu-specific mutation fence. In-process
applications retain preimages for explicit `rollback`; preimages are never restored or replayed.
A fully applied option that fails verification remains in place. Its report identifies the application,
states whether changes remain and exposes the error; only a partial write failure triggers the
ordinary executor's automatic compensation.

Menu action receipts enter ordinary incident repair evidence. A terminal menu success is not independent
broker closure or positive program trust credit. Controller health verification, broker ACK and the
benchmark adapter's strict receipt remain separate evidence. See the
[analysis reference](../.agents/skills/analyze-experiment/references/mitigation-menu.md).

## Benchmark fault and result boundary

`benchmarks/sregym/faults/description.py` owns the versioned
`sdo.sregym-fault-description/v1` metadata contract. Its harness subprocess
resolves registered singles, catalog composites (excluding decoys), and generated
co-located composites without cluster access. Descriptions preserve declared
indices, actual injection order, class/key, parameters, probe availability, and
required repair/snapshot scope. The runner and both fastloop selection paths
validate this scope before deployment. Namespace labels/PSA, ResourceQuotas,
admission webhooks, and unreviewed fault classes fail this check; production
Roles and informers remain application scoped.

Fastloop records `fault_descriptions.json` alongside incident results. Composite
analysis reads that recorded catalog, avoiding a second hand-maintained fault
map or dependence on the analyst's current harness checkout. Unsupported probes
use the official aggregate oracle and record `tracking_supported=false` with no
per-fault resolution times. `--require-fault-tracking` rejects such runs before
deployment. Hidden identities, injection details and oracle answers stay in this
benchmark boundary and never enter production tasks or operational memory.

The sdo3 v2 worker and deferred HTTP gate expose explicit injection validity.
Invalid injections stop before response/receipt polling; the persistent adapter
pauses maintenance and records benchmark infrastructure evidence. Fastloop
performs full cleanup, records an infrastructure row without learning credit,
and excludes it from incident summaries. Full recovery errors stop the warm
stream, including continue-on-error runs. Component addressing flattens every
worker-returned index; official aggregate oracles retain collateral/traffic policy.

The pipeline accepts terminal infrastructure rows in both per-problem and
`run_<attempt>` layouts, records their state as `invalid_infra`, and preserves
resume and workspace chaining. Such rows require no agent verdicts or strict
receipts; invalid injections still require injection evidence. Analysis checks
the top-level `infra_error` before token, submission and receipt requirements,
excludes these rows from agent scores, and preserves independently graded
aggregate failures with only per-fault infrastructure details. Composite metrics
retain fault counts, injection validity and per-fault details beside existing
aggregate success and timing. The harness owns selection and validation of its
immutable checked agent image; the adapter no longer probes an obsolete tag.

## Controller

The controller is split by stability and privilege:

| Package | Responsibility |
|---|---|
| `sdo/operator/sdk/` | Public Go interfaces for detector specifications, snapshots, findings, persistence, batching, and test snapshots |
| `sdo/operator/sdk/program/`, `sdk/sdktest/` | Shared detector/Diagnose reads, deterministic finding-bound roles, typed plans, namespace-scoped capture with read recordings, and offline repeat-path replay |
| `sdo/operator/sdk/servicehealth/` | Zero-knowledge ready-endpoints detector that names the selector labels a Service's backing Deployment lacks, and the Service reachability detector, its target derivation and dial-observation types |
| `sdo/operator/sdk/workloadhealth/` | Zero-knowledge pod and Deployment state detector (failed, pending, unschedulable, crash-looping, image-pull, config-error, OOMKilled, unready, restarting, stuck-init pods; unavailable or deadline-exceeded Deployments) whose ages use the snapshot observation time |
| `sdo/operator/sdk/traffic/` | Synthetic-traffic scenarios, parameter generators, workload profiles, the deterministic engine, sliding-window SLO evaluation, and the scenario SLO detector |
| `sdo/operator/sdk/metrics/` | Optional metrics input: PromQL query declarations, frozen results, the threshold detector and the firing-alerts detector |
| `sdo/operator/core/` | Detector execution, snapshot validation, finding emission, and playbook-path validation |
| `sdo/operator/runtime/` (including `jitview/`) | Kubernetes cache, scheduler, finding state, batching, playbook coverage, responder jobs, incident-scoped program selection and loop, plan execution, broker effects, leader election, durable state, the credential-free prober pod with namespace-scoped Service and captured-Pod TCP observations, and the Service reachability observer that derives dial targets from the snapshot |
| `sdo/operator/runtime/prober/` | The isolated routing and synthetic-traffic prober process (no Kubernetes dependency), also built with zero workloads: continuous health probes, verify bursts, one-off read-only probes, stateless Service reachability dial rounds, HTTP API |
| `sdo/operator/runtime/prometheus/` | Prometheus HTTP API adapter (`/api/v1/query`, `query_range`, `alerts`) behind the runtime's transport-neutral `MetricsAPI` |
| `sdo/operator/builder/` | Validate diagnostics, playbook programs, shared libraries, and corpus cases in an isolated workspace; compile the application-specific controller and prober, or one working playbook for agent-requested execution without publication |

Generated detectors are deterministic Go. They consume controller snapshots and must not call models, read hidden benchmark labels, or use external verdicts.

### Scoped observation

The installer owns a versioned `ObservationScope` ceiling, separate from memory and ambient Kubernetes permissions. `host --observation-ceiling-json` accepts trusted JSON with `version: 1` and explicit `targets` (`api_version`, `kind`, `namespace`, or a cluster object's `name`). The installer validates the ceiling, provisions it as an explicit controller/operate environment value, includes it in the install fingerprint, and grants read access only to the runtime controller and trusted Go bootstrap observer. Foreign namespace Roles permit Deployment, Service and Endpoints reads; admission webhook ClusterRoles permit GET only for individual declared names. These grants add neither foreign Secret reads nor responder repair privileges. A missing ceiling permits no additional observation. Reinstallation changes policy; accepted memory cannot change it.

Architecture front matter may declare `observation_scope` version 1 with `namespaces: [{name, kinds: [{api_version, kind}]}]` and `cluster_kinds: [{api_version, kind, names: [...]}]`. Namespace kinds are limited to `apps/v1 Deployment`, `v1 Service` and `v1 Endpoints`; cluster kinds are the two `admissionregistration.k8s.io/v1` webhook configurations, by explicit name. Unknown versions/fields, wildcards, duplicate targets and more than 32 targets are rejected. Broker validation checks declarations against its own immutable ceiling and checks detector watches against the architecture. Proposal self-checks receive an advisory copy in private Git metadata; forged copies and proposal environment values never override broker authority. Existing architecture ownership still excludes responder source and reflection proposals.

The runtime reads the architecture from an immutable accepted Git revision. Its generation binds that revision and the canonical scope/ceiling identities. Legacy SDK accessors remain application local. The optional `ScopedObservationSource` provides copied, sorted, credential-free scoped objects and per-target availability. Scoped reads use fresh, bounded LISTs and named webhook GETs at snapshot time; their cache retains projected prior objects only for comparison, and emits qualified scheduler events. Each target has a two-second deadline, a 500-object cap and a shared two-MiB projected-output ceiling; failed targets retry with per-target exponential backoff capped at thirty seconds. This polling path needs no foreign watch or cluster-wide webhook list privilege. Application-local informers are unchanged.

Successful empty lists and named GET NotFound mean observed absence. Authorization failures are `forbidden`, missing APIs `unsupported`, and other failures/caps `unavailable`. Error bodies are omitted. Failed targets return accessor errors and declared detector dependencies remain Unknown/error, so they cannot clear findings. Healthy baseline promotion requires complete scoped coverage, preserves the previous state during gaps, and compares only observed targets after a fresh read. A new controller/scope generation establishes its own baseline without synthetic object changes.

Foreign Deployments expose replica counts, selector/template labels and container name/image/ports/resources; environment values and references, commands/arguments, volumes, probes, annotations and arbitrary status messages are excluded. Foreign Services retain routing identity, selector and ports. Endpoints retain sorted ready/not-ready addresses and ports without target references. Webhooks retain rules, selectors, policies, timeouts, service identity and URL origin; URL userinfo, path, query and fragment are removed, service paths and match expressions are omitted, and CA bytes become a SHA-256 marker. Bounded v1 limitation: changes only to webhook service/URL paths or match conditions are invisible to watch comparison, source drift and replay. Those fields remain excluded from the credential-safe projection; observed equality is limited to the retained fields. All new paths share these projections before SDK access, drift, baseline or capture publication. Read-only source comparisons normalize the observable Kubernetes defaults without foreign dry-run writes. Scoped observations add no Secret access. Application-local ConfigMap values remain omitted from captures only.

Captures persist `scoped_observation` version 1 with accepted generation, scope, target statuses and projected objects. JSON replay exposes the same optional interface without network access. State changes carry API version and namespace in runtime and protobuf projections; cluster namespace remains empty. Reviewed webhook configuration changes, including source-absent creates/deletes, require successful accepted-source lookup. Read-only qualified lookup preserves foreign/cluster identities from the accepted source selection; it grants no foreign image fallback or apply authority. Initial reads are suppressed. Endpoint turnover wakes dependencies and supplies baseline evidence without opening configuration drift incidents. This increment adds no metrics, collector or resource-health detector.

### Synthetic traffic

The health judge writes Go generators (`.sdo/diagnostics/traffic/generators/`, a package exporting `Scenarios() traffic.Catalog`) and workload profiles (`traffic/workloads/<name>.yaml`). A scenario is a user journey: steps whose endpoints build requests from a seeded rng and per-iteration state and check responses, the Services on its request path (from `.sdo/arch.md`, for localization), a side-effect class (read, idempotent write, or write with cleanup steps), a synthetic-data marker for writes, and the fault classes it must detect. A workload is data: purpose (`health-probe` runs continuously and feeds a traffic health detector; `verify-burst` and `journey` run on demand for a bounded time), scenarios and weights, arrival pattern and rate, timeouts, and per-scenario SLOs. Its schema is defined once in `sdo/contracts/proto/sdodev/contracts/v1alpha1/traffic.proto` and code-generated for both sides; protovalidate CEL carries the invariants that do not need a Go-duration parse (duration bounds stay host code).

The `traffic.Engine` owns scheduling, arrival, rate, in-flight and time caps, latency measurement, and seeding: iteration *n* of a workload always builds the same requests, so any failing probe replays exactly. It refuses unsafe requests (writes from read scenarios, writes without the marker) without sending them. The builder compiles generators only into a separate prober binary (`cmd/prober`, with workloads embedded), never into the controller; generated tests prove that every scenario fails against simulated unreachable and 5xx targets plus its declared classes, that workloads name provided scenarios, and that traffic detectors consume health-probe workloads, and a static guard rejects generator imports that reach the network, filesystem, or cluster and package-level clock or rng calls.

The builder always emits the routing prober, including for applications with no traffic generators or workloads. The controller initializes its read-only probe API independently of traffic consumers and awaits pod readiness before detection; `--synthetic-traffic=false` disables background workloads while retaining routing probes. The prober fingerprint includes that workload mode so a mode change replaces the pod. When routing probe capability is unavailable, the incident capture contains a typed `routing-probe-note.json` (`capability: routing_probe`, `status: unavailable`, reason, first affected phase and optional error detail), and the controller logs the gap once per incident. Probe errors remain errors rather than refused connections. The controller runs the prober as a pod in the control namespace: no service-account token, a read-only mount of only its content-addressed binary on the repository volume, CPU and memory limits, restart on crash, and a NetworkPolicy allowing egress only to the application namespace and cluster DNS. An unchanged binary keeps its pod across controller relaunches. The controller polls the prober's windows every 500 ms and wakes the traffic detectors only while a failure is inside a scenario's window; a scenario judges health only after it has succeeded once, and an unreachable prober surfaces as a detector error that blocks closure but never opens an incident. The always-on `service-endpoints` and `workload-health` detectors need no authoring. Neither does the always-on `service-reachability` detector (`servicehealth.NewReachabilityDetector`), a blackbox port probe: every 10 s the controller derives one target per TCP port of every cluster-IP Service with a selector from its own namespace snapshot (headless, ExternalName, selector-less, UDP and SDO-managed Services are skipped; at most 64 targets) and posts them to the prober's stateless `/v1/dials` endpoint, which dials each through cluster DNS with a 1 s timeout and classifies failures as refused, timeout, dns, reset or unreachable. The controller keeps the history and wakes the detector with the `sdo.dev/v1alpha1 ServiceReachability` watch; the prober stays credential-free and lists nothing. The detector fires `service-port-unreachable` on a Service when its last 3 dials of a port, all made after the Service gained a ready selected pod (the earliest Ready transition among its ready pods), failed. No prior successful dial is required, so a port that refuses from the start while pods report Ready is reported; without a ready backend `service-endpoints` owns the fault. Evidence names the port, targetPort, declared container ports (with a hint when no container declares the targetPort), the error classes and the last successful dial. Blind spot: the prober is not an application pod, so the detector stays quiet for a port when an ingress NetworkPolicy selecting the backing pods does not admit the prober there (ipBlock peers and unknown namespace labels count as not admitting), and names admitting policies in the evidence. `--evaluate-once --prober-url` collects the same history synchronously (three rounds one second apart through `/v1/dials`) before judging, so the bounded health command reports the same rule; without `--prober-url` it reports command status `unavailable` with the affected detector IDs and reasons, which cannot establish seed health, and `--run-once` has no prober, so reachability (like synthetic traffic) reports nothing there. Controller snapshots and corpus captures record their observation time, which `sdk.ObservationTime` exposes so age-based detectors stay replay-deterministic.

The native `--healthy-baseline` opt-in also enables a trusted pre-dispatch reachability baseline gate. With no restored incident or pending closure, it collects fresh dial rounds while independently evaluating every other health detector, including the authored objective and traffic. Only measured failing targets in that independently healthy state become exclusions. The gate writes an isolated health-judge proposal and publishes it through the broker at `.sdo/diagnostics/detectors/health/service-reachability/baseline.json`, then requires detector-bundle adoption before dispatch. The immutable generic Go policy remains unchanged. Empty records avoid repeated capture; restarts consume accepted JSON. Each exclusion binds the accepted application-source hash and objective digest to the Service identity/spec, selected backend container configuration, referenced ConfigMaps and selecting network policy. Changed source, objective, targetPort, selector or backend/config/policy invalidates the exclusion; ordinary incidents and previously unobserved targets retain normal detection, including targets with no successful dial. Findings expose invalidation through per-port `baseline_status`. Secret resources are never read by this gate. The diagnostic remains health-judge-owned under the existing health-detector directory ownership rule; responders cannot publish exclusions.


### Metrics

Detectors may consume high-level metrics from a Prometheus-compatible API, an optional input that follows the synthetic-traffic precedent. A detector declares `metrics.Query{ID, Expr, Range, Step}` through `metrics.Consumer` (and `metrics.AlertConsumer` for alerts) and reads frozen results with `metrics.ResultFrom`/`AlertsFrom`; it never does I/O. With `--metrics-url` (and optional `--metrics-token-file`, read on every request), the runtime's `MetricsObserver` evaluates each declared query once per evaluation cycle at one timestamp, concurrently, with a per-query timeout (5 s) and bounds on query count (32), series and alerts, and freezes the results into that cycle's snapshot (`MetricsSnapshotProvider`). A cycle whose detectors read no metrics sends no request. `runtime` sees only the `MetricsAPI` interface; Prometheus wire handling lives in `runtime/prometheus/`.

"Metrics source unavailable" is a distinct state. A timeout, 5xx, malformed response, oversized result or missing `--metrics-url` becomes a per-query (or alerts) `Error`, which detectors return as a detector error: it never counts as healthy, so it blocks closure, and never as firing, so it opens no incident. `evaluate-once` requires `--metrics-url` when a detector consumes metrics. `metrics.NewThresholdDetector` compares each series' (last, min, max or average) value with a threshold and maps labels (defaults `namespace`, `deployment`, `pod`, `service`) to the primary Kubernetes resource; findings carry the query, value, threshold and labels as evidence, which incident requests retain. `metrics.NewAlertsDetector` reports one finding per firing alert whose namespace label names the application namespace; pending alerts are ignored and alerts without the label are counted in finding metadata. Detectors declare the `metrics.Watch` pseudo-kind and run on their interval; the runtime does not emit it. Captured corpus snapshots do not include metrics, so metric detectors are checked with `sdktest.Snapshot.Metrics`/`Alerts` fixtures rather than corpus replay.

Opt-in: the default install is unchanged and needs no metrics. When an operator passes `--metrics-url` to `operate`, `host`, `install-existing` or the controller (`install.Config.MetricsURL`, `ControllerConfig.metrics_url`), the controller receives it, and the broker and the judge's `memory check` (`SDO_METRICS_DETECTORS`) install the generic `metrics-alerts` health detector through `memory.PrepareHealthWith`. Threshold detectors are application-specific and judge-authored.

## Operational memory

The application repository is the shared durable memory. Six artifact classes live under `.sdo/`:

| Artifact | Owner | Contract |
|---|---|---|
| `goal.md` | Human | Application identity and exact health objective |
| `arch.md` | Deployer/upkeep | Source commit, topology fingerprint, complete architecture summary and optional observation scope bounded by installer policy |
| `playbooks/`, `lib/`, `corpus/` | Responder working checkout for draft programs/helpers; broker-validated reflection for publication | Fault-specific judgement guidance and optional typed Go programs, their shared helpers, and labeled replay cases; these are one playbook-memory class. Service-targetPort-only mitigation replay may retain actual direct-Pod TCP observations for unchanged captured Pods; Service probes, fixed answers and reads after other effects are never reused as post-plan evidence. The worker writes provenance for new cases; shared validation checks the provisioned capture context and preserves every accepted case against the base commit. Shared validation replays incident detectors on recorded faulty and healthy states, derives bindings through production candidate selection, requires Match, and executes mitigation on that same binding. SetField-only plans must repair the open fixture to NoMatch. Unavailable external effects fail repair replay and require a typed SDK gap. Verified success and failure counts inform the responder. |
| `diagnostics/` | Health judge through direct edits; responder in its isolated reflection checkout | Health and incident detector source, tests, module, and manifest; reflection writes responder ownership, incident class, and accepted task provenance for independent broker validation. Synthetic-traffic generators and workloads remain judge-owned, except responder-owned `traffic/generators/incident/` and `traffic/workloads/incident-*.yaml`. |
| Outcomes | Controller through the broker | Immutable, versioned incident outcome documents and evidence |
| `rendered/` | Go controller through the broker | Accepted per-object deployment YAML, container image strings as source writes them; cluster objects retain cluster identity and are looked up through accepted source. Carried unchanged by memory-only commits |

The builder stages `.sdo/diagnostics`, `.sdo/playbooks`, and `.sdo/lib` into one temporary Go module using the diagnostics module path. Authored packages import shared helpers as `<diagnostics-module>/lib/<package>` and the SDK as `sdo.dev/operator/sdk`. Detector-only repositories retain their existing `diagnostics/go.mod` without migration. Corpus data stays at `.sdo/corpus/<case>/`; shared libraries may be proposed independently. New programs need an observed matching case with resource bindings and origin incident in `labels.json`; capture digests are optional. An agent-added synthetic NoMatch case uses `no_match_program_ids` and `synthetic_no_match_reason`. The isolated validator checks imports, program registrations, bindings, executed mitigation replay, and shell-free program guidance. These policies and generated replay tests live in `memory/validate/program`; the builder consumes their validated specification. Immutable accepted cases and changed-source formatting are semantic rules; program, library, and corpus ownership is in the same declarative ownership table. `program scaffold` creates final files directly; `memory check` uses the same validator and compiler body as broker acceptance.

There is no rendered deployment lock and no image evidence. `AcceptedManifest(s)`, adoption, drift, `ReconcileFromSource`, `sdo source` and closure render accepted source directly from Git, like `kubectl diff` and `kubectl apply`. Each container's image string is compared and applied exactly as source writes it (tag or digest); no deployer receipt or image ledger is required for any workload kind. A source rendering failure for one object is explicit unavailable evidence for that object, never confirmed absence. Available objects remain readable and adoptable beside failed objects, and a complete source inventory can still establish that an unrelated foreign object is absent.


The logical outcome contract is `OutcomeRecord` in `sdo/contracts/proto/sdodev/contracts/v1alpha1/outcome.proto`. The controller derives it, the broker alone receives `memory.Writer`, and `memory.Reader` exposes `Get` and `List` by incident ID. Reflection receives the accepted record and prior records as typed protobuf fields. The Go JSON storage adapter alone maps an incident ID to `.sdo/outcomes/<sha256(incident_id)>.json`, stores the versioned document, and can import the prior `.sdo/outcomes.jsonl` stream once, deleting that stream only after every record is accepted. Documents written by an earlier build stay readable: the wire layer maps retired field names and retired string or enum values to their successors (`contracts/wire/retired.go`, guarded by snapshots of every field and every value a stored message has had, so removing or renaming one without a mapping fails a test even when the update flag is set; the frozen reflection task, rejection log and action receipts read through the same migration), and `List` skips an outcome that still cannot be read, reporting it through `ListWithNotes`/`memory.ListOutcomes` (which logs each skip) instead of failing every reader. A read of one incident by ID (`Get`, the write path) still fails on an unreadable document. Consumers never infer physical filenames. Closure phase timestamps, reflection skip reasons, and typed learning failures are kept in the durable broker incident ledger because the outcome document is committed before reflection and is immutable. The deployer receipt lives outside `.sdo/` in its own durable ledger bound to an accepted commit; it is information only.

`OutcomeRecord.classification` in `outcome.proto` defines four outcomes, shared by generated Python readers and parity-checked Go constants: `failed` for responder failure or unverified health, `cleared_without_action` for verified health without an accepted source repair or successful repair action, `unconfirmed` for a verified closure whose closure-time recheck never saw a complete detector observation window, or whose observer stayed unavailable, within its bound (the detectors' clearing horizon plus two cadences; `IncidentClosure.unconfirmed_reason` is `OBSERVATION_INCOMPLETE` or `OBSERVER_UNAVAILABLE`), and `success` otherwise. The recheck has a typed verdict: an incomplete window or an unavailable observer (timeout, API 5xx, unsynced) waits; a denied observer (RBAC, undeclared or unsupported dependency) fails the closure with `CLOSURE_FAILURE_REASON_OBSERVER_DENIED`; only an active finding, unexplained inventory, unrestored source fields or a detector missing from the adopted policy supersede it. Unconfirmed outcomes are neither success nor failure evidence; reflection still runs on them. Successful outcomes are eligible for reflection and prior-outcome recall. Strict receipts accept success and clearing without action, retaining health, broker commit, incident identity, reflection and rollout checks. Diagnosis citations are recorded claims; neither Go derivation nor receipts independently verify them. Program trust still requires successful outcomes and controller-recorded, revision-bound applications and post-repair NoMatch evidence, including equivalent repairs recorded through `sdo act`.

`sdo/operator/memory/layout` is the only Go package that knows these physical paths: it owns the path constants and builders, `Classify(path)` (kind, ID, ok), the per-kind parsers and typed `Tree`, and the single detector manifest type. Each detector carries its own `manifest.yaml` at `.sdo/diagnostics/detectors/<health|incidents>/<id>/manifest.yaml`; the directory name is the detector ID. Schema version 2 introduced this layout; schema version 3 refers to playbooks only by logical ID (`DetectorSpec.Playbooks`, `Finding.Playbooks`, and `possiblePlaybooks` hold IDs, never paths). A version 1 repository (single `.sdo/diagnostics/manifest.yaml`) or version 2 repository is converted by `layout.Migrate`, which rewrites physical playbook paths in detector Go source and manifests to IDs, which the Upkeep actor commits through the broker. The controller does this at every start (operation `memory-v<N>-schema`, idempotent) before its first memory read, and `operate` does it as its schema step.

`sdo/operator/memory/validate` is the one validator for `.sdo/`. `validate.Validate(ctx, Request{Root, Base, Actor, Phase, Layers})` returns `[]Diagnostic{Rule, Path, Line, Message, Hint}`; validation failures are diagnostics, and the error result is reserved for infrastructure failures (unreadable files, a missing toolchain, a bad request). Rule IDs are stable strings such as `layout.unknown-path`, `manifest.parse`, `ref.unknown-playbook`, `ownership.denied`, and `detector.compile`. Acceptance always calls `Check` with all four layers; `Validate` exposes individual layers to internal tooling and tests:

- **Structural**: every `.sdo/` path classifies through `layout.Classify`; no symlinks, secret-bearing names, or files over 256 KiB (corpus files have a separate 8 MiB cap); YAML, front matter, manifests, and Go syntax parse; `schema-version` is current. Changed source files outside `.sdo/` are syntax-checked too.
- **Semantic**: ID patterns, unique detector IDs, `possiblePlaybooks` that resolve, playbook front matter, a complete playbook index, scripts that are executable, syntactically valid, and named by their playbook, no physical `.sdo/` paths in the architecture, playbook index, playbooks, scripts, or detector Go source, and an initial architecture that cites the accepted source commit. Completeness (goal, architecture, at least one playbook and detector) is required except in the bootstrap stages before the health judge.
- **Ownership**: computed from the diff against `Base`. The policy is one declarative table of kind by actor by phase (`grants` in `ownership.go`) plus diff rules for detector provenance, protected detector classes, Upkeep schema migrations, and the one-file architecture stage. The broker no longer has its own path-prefix checks.
- **Compile**: detectors build against the SDK, pass `go vet`, and pass their own tests, reusing the builder's detector workspace. It executes detector code after the other layers pass. The broker uses the network-isolated validation Job (`sdo-operator validate`); agent self-checks use the same runner body locally.

The broker's `checkCandidate` calls `validate.Check` on its staged clone. That function runs structural, semantic and ownership checks, then the configured compile runner. The broker supplies its isolated Job runner, whose body is `validate.CompileCandidate`; the CLI uses that body locally.

`sdo-operator memory` (`operatorcli/memory*.go`) uses the same validator and layout for agents and tools. `memory check [--root] [--json]` calls `validate.Check`, the broker's complete acceptance pipeline: structural, semantic and ownership checks against `SDO_BASE_COMMIT`, followed by build, vet, detector tests and program corpus replay. Session fixtures do not require the private capture mount. `SDO_ACTOR`, `SDO_PHASE` and `SDO_BASE_COMMIT` are required; operator sources come from `SDO_OPERATOR_ROOT` (default `/opt/sdo/sdo/operator`), and `SDO_HEALTHY_BASELINE` supplies recorded healthy snapshots when configured. The compile runner body (`validate.CompileCandidate`) is shared with the broker's isolated validation Job. Exit codes are 0 when clean, 1 for diagnostics, and 2 for infrastructure or usage errors, including missing inputs. `--json` prints the existing `MemoryCheckReport` through `validate.Diagnostic.Proto`; non-blocking coverage gaps (verified cause without a program, a program no detector lists, a skipped healthy replay, a recorded fault the detectors do not reach) are in `coverage_gaps` and print as `coverage_gap` lines in text mode. The validator Job returns the same gaps in its framed report, the broker records them as `SDO-Coverage-Gap:` trailers and in `Receipt.CoverageGaps`, and the controller stores them in the incident ledger's `reflection_coverage_gaps`. `memory ls` lists logical IDs, `memory new playbook|detector <id>` scaffolds files, and `memory outcomes` prints stored outcomes. `broker/parity_test.go` compares the real CLI subprocess with broker acceptance, including compile, vet and detector-test failures, and requires identical verdicts and diagnostics.

Responder traffic: responders own incident-scoped traffic (`traffic/generators/incident/` and `traffic/workloads/incident-*.yaml`) in the reflection phase, and the health judge owns all other traffic. This is the broker's long-standing behavior and what the ownership matrix below documents. Both roles edit final checkout files directly; the proposal writer has been removed. The shared check prepares generic health policy detectors explicitly.

`schema-version` and `lifecycle-provenance.yaml` are validation metadata. They do not change the six ownership classes. Lifecycle provenance may attest an exact diagnostics-tree digest against an immutable validator image identity; only that exact pair can reuse a prior successful compilation, while any detector or validator change forces isolated validation again.

Go broker seeds may omit the historical Python lifecycle-provenance file. The
benchmark adapter recognizes a current-schema goal, architecture and health
bundle for seeded installation; the Go installer still independently validates
that memory before starting the controller.

The storage edge fingerprints all accepted regular source blobs, streaming
non-manifest assets without retaining their bytes. YAML parsing keeps a 64 MiB
aggregate budget; large binary assets do not consume that parsing budget. The
fingerprint encoding and exclusion of operational-memory files remain unchanged.

### Detector firing telemetry

The controller records, per finding, whether and when each detector fired. The runtime appends `activated`, `cleared`, `not_persisted` (flagged but gone before the detector's firing threshold) and `batched` (the activated finding joined a dispatched incident) records to a JSONL stream, `detector-firings.jsonl`, in the controller's durable runtime area on the repository PVC (`/workspace/.sdo-runtime/telemetry/`, `--firing-telemetry-path`, default on in job mode). It is operational telemetry, not operational memory: it is outside `.sdo/`, changes none of the six artifact classes or their ownership, carries no benchmark labels or verdicts, and is derived only from the controller's deterministic finding state. Each record has the detector id, class, owner, rule id, fingerprint, severity, parameter bindings, surfaced playbooks, firing/clear counts against the persistence policy, the evaluation iteration, the open incident id (empty if none), and `dispatch_relation`: `before_dispatch` (part of the dispatched batch), `after_dispatch` (activated while the responder was in flight) or `no_incident`. Because the batcher debounces, an activation usually records `no_incident` and a later `batched` record settles `before_dispatch`.

Event ids are a hash of the detector, fingerprint, event kind and a per-finding transition counter kept in the durable controller state, so a replay after a crash between emitting and persisting reproduces the same ids and the sink drops the duplicates; an ordinary restart restores the counters and emits nothing. The file is bounded (4 MiB, rotated once to `.1`, so at most 8 MiB), a torn last line is repaired on open, and a stream left over from an earlier controller lifecycle is archived as `.prev` when no state is restored. Sink failures are reported and never stop the evaluation loop.

The controller also keeps a compact timeline in its state and puts it in the incident closure as `detector_timeline` (per finding: `first_activated_at`, `last_seen_at`, `cleared_at`, `relation`, bindings, `surfaced_playbooks`) with three derived booleans: `incident_detector_fired_before_dispatch`, `incident_detector_fired_after_dispatch`, and `no_incident_detector_fired` (only health detectors fired). `IncidentClosure` carries them and the broker validates them and the SREGym adapter copies them into the strict receipt (`detector_firing_available`, `detector_timeline`, the booleans), exports the stream into the stage results as `detector_firings.jsonl` and `sdo_runtime/telemetry/`, and `stream_curve` reports them per incident. They distinguish a learned incident detector that fired before dispatch (memory used) from one that never fired (memory unused).

### Incident lifecycle event stream

The firing stream records whether and when detectors fired; the incident lifecycle event stream records the controller's own lifecycle transitions, so an operator can reconstruct a single incident's full timeline offline. It is a second durable, append-only JSONL stream (`lifecycle-events.jsonl`, `--lifecycle-telemetry-path`, default on in job mode) in the same runtime telemetry area (`/workspace/.sdo-runtime/telemetry/`), sharing the firing stream's durability mechanism: a bounded 4 MiB file rotated once to `.1`, a torn last line repaired on open, a stream from an earlier lifecycle archived as `.prev` when no state is restored, and content-addressed event ids that make a crash-replay between emitting and persisting idempotent. Each line carries the same envelope as a firing record (`schema_version` `sdo.lifecycle-event/v1`, `event_id`, `event`, `recorded_at`, `application`, `namespace`, `incident_id`) plus phase-specific fields. The `event` phases fill the transitions that previously emitted nothing: `incident_opened` (with `finding_keys`), `workspace_prepared` (`worktree`, `base_commit`), `responder_dispatched`, `responder_completed`, `incident_closed` (`health_cleared_at`), `closure_committed` (`proposal_commit`, `outcome_commit`, `reflection_commit`, which join the logical outcome by incident ID or outcome commit; the active Go memory edge stores per-incident `.sdo/outcomes/*.json`), and `incident_acknowledged`. Each `(incident_id, phase)` pair is emitted at most once, so its id is `sha256(incident_id, phase)`. `sdo/operator/runtime` stays transport-neutral: every field is a controller-internal fact (incident ids, git commit shas, worktree paths, finding keys), carrying no benchmark labels or verdicts, and the stream is derived only from the controller's deterministic lifecycle state. `ReconstructIncidentTimeline` joins the lifecycle and firing streams by incident id into one deterministically ordered timeline for offline analysis, and the record-and-replay harness pins that reconstruction against frozen artifacts.

#### Late findings (pull-before-act)

The incident request is immutable once the responder launches, so a detector that activates later cannot join the original batch. With `--late-findings pull`, the Go controller maintains a bounded per-incident report in a controller-owned ConfigMap. The controller projects that report into the session's read-only `late-findings` directory (the same mount for warm and cold workers) and the incident worker receives only that projection, without controller API credentials, and invokes `sdo-operator late-findings --incident-id ID`. The command returns logical detector and playbook IDs and writes a bounded pull receipt. The broker checks the worker's typed pull claim against controller-owned telemetry before recording it in closure evidence.

Detectors and Diagnose use `program.ReadView`; detectors obtain it with `program.ReadFrom(context)`. Detectors have independent detection entry points; playbook Diagnose runs only while handling their findings. Required roles bind once from that finding, and Diagnose reads additional objects by name or relation. This surface includes namespace/scoped objects, optional workload objects, frozen metrics and alerts, traffic windows, logs, TCP/HTTP probes, and accepted-source/baseline diffs. `QueryMetrics(metrics.Query)` also supports on-demand instant/range PromQL reads when a metrics URL is configured. Existing metric declarations and ID accessors remain supported. Only the runtime performs I/O. Its `ReadSnapshotProvider` creates a fresh `CycleReadView` per snapshot, deduplicates identical primitive reads (including failures), and records each logical use. Logs keep the 64 KiB / 5 s cap and probes the 5 s cap. Independent failure guards make ignored primitive/metrics errors Unknown for Diagnose and errors for detectors, so unreadable inputs cannot clear a finding; a successful negative probe remains an observation.

Captured snapshots add optional `metrics`, `alerts`, `traffic`, `reachability`, and `readings` fields; legacy object-only snapshots remain loadable. Snapshot readings hold only actual reads from that snapshot, including errors. Later Diagnose reads stay in the existing phase-specific primitive recording files. sdktest and runtime/replay load both, retaining phase isolation; absent evidence is unavailable, never healthy. Detector replay uses the recorded source/baseline diff, while closed-loop program replay retains its independent accepted-source comparison and never borrows fixed probes/logs/metrics to prove a post-plan effect. No artifact owner or production/benchmark boundary changes.

Selected local Kustomize deployment units render from tracked inputs at the accepted Git revision, including file-generated ConfigMaps and rewritten consumer references. Rendering uses a private in-memory filesystem and no executable, remote, or Helm plugins. Secret generator inputs are never loaded: stable-name Secrets remain identity references, and hash-dependent Secret names fail closed. Raw Secret manifests are also projected to identity references before recording. Benchmark lifecycle-created objects need an explicit declaration in accepted application source; production does not infer generators from benchmark code.

Typed structured plans preserve rollback preimages and patches alongside object effects; the protobuf receipt carries action/effect evidence. `CreateFromSource` restores a missing named object from the incident-bound accepted manifest, while `DeleteObject` may remove a named object in the incident namespace even if it remains in accepted source. No separate source policy, content predicate, credential-delta rule, lifecycle kind allowlist, or policy digest is required. UID and conditional-write checks, direct observation, exclusive lifecycle ownership within one batch, independent verification and rollback remain. Delete effects stay explicitly irreversible and are never auto-recreated. Restoring a referenced config object can recycle blocked managed Pods once; stale Service consumers are recycled only after a selector-related backend is Ready. Both use the existing rollout budget and UID/version preconditions, with irreversible Pod deletion effects retained in the repair receipt. Source reconciliation restores every source-declared configuration field, including RBAC fields, and preserves undeclared server defaults and controller metadata. The accepted application render lock qualifies named cluster source independently of namespace identity. See [automatic repair checks](plan-safety-authority.md).

Incident responders never merge directly into the operational branch. The broker creates isolated worktrees, checks path ownership and append-only rules, runs repository and detector validation, and accepts only attributable commits. Two repair-evidence policies are supported. `commit` retains a proposal commit for every response, including an empty attribution commit. `recorded-actions` permits a live-only repair without a proposal commit when the responder returns a structured, successful action receipt; any repository changes are still validated and committed. In both modes the controller commits the authoritative outcome and reflection, and independently owned health detectors must verify recovery. The source deployer separately authors attributable deployment commits during the initial lifecycle. Health detectors remain judge-owned. Responders can edit playbook/helper drafts during response; only validated reflection publishes those drafts and incident detectors after closure.

## Cluster permissions

SDO does not limit its identities through Kubernetes RBAC. The installer creates
one ClusterRole, `sdo-admin-<control-namespace>`, with `*` on `*` for `*`, and
one ClusterRoleBinding to every ServiceAccount SDO runs as: `sdo-controller`,
`sdo-operate`, `sdo-responder`, `sdo-image-builder` (control namespace) and
`sdo-script` (application namespace). In a separate control namespace it also
binds the user the controller impersonates for the checked removal callback.
There are no per-kind Roles, no readable-versus-writable table and no
vocabulary-versus-Role ratchet, so a repair or a replayed program is never
refused for a missing grant (a Deployment, an Ingress, a webhook configuration
or a Namespace label are all reachable). What a program or responder may change
is decided by the typed plan checks, review and runtime policy, not by RBAC.

A fresh `host`/`operate` install, `install-existing` and `inspect-existing` all
re-apply the ClusterRole and binding, so a narrowed or deleted grant is restored
on the next install or inspection. The grant is cluster-wide, so recreating the
application namespace between stages does not remove it. `--permissive-responder`
and `--responder-namespace` keep their meaning for the runtime (which namespaces
the responder may repair in, and creating the fault-backend namespaces) but add
no separate grant. The read-only detector runner Pod keeps its own narrow Role.
Credential hygiene is separate from RBAC: Secret contents never reach logs,
prompts or memory.

## Controller/responder interaction

The controller batches persistent findings into an incident request. The request includes the health objective path, architecture path, findings, surfaced playbooks, detector history, an isolated repository worktree, and the selected repair-evidence policy. A responder returns a structured result with diagnosis, applied playbooks, repository changes, and per-action receipts recording the target, timing, success, and reversibility of live mutations. Independent health detectors must clear after the response before closure. Drift opens incidents but never decides verification or closure. The controller still compares admitted owned fields and the accepted-source inventory with the live namespace, and reports what differs: `verify` and `status` carry it in `drift[]`, the continuation prompt in `source_differences`, and the closure outcome in `source_check_notes`. If health is clear the incident closes as success; if health does not clear by the configured verification timeout it closes as `failed`, whatever the drift state. Only pending deployment rollouts still hold a closure, because health samples are not settled while they run. The drift findings and events stay in the outcome. Late drift still attaches to the running responder through `attachDrift`, its moving incident view and the late-findings report. Terminal responder failures retain their existing failed closure path without treating unresolved drift as verified recovery.

Before creating a responder Job, the controller freezes its exact canonical ProtoJSON task, including the logical memory snapshot and initial program runs, in the pending runtime state. A task over 16 KiB is written first to `.sdo-runtime/requests/<job>/prepared-task.json` on the repository volume and the state ConfigMap holds only its `staged-task:sha256:<digest>` reference; a restart resolves the reference in memory and rejects missing or altered bytes. Verbose incident records above 32 KiB are frozen as content-addressed canonical ProtoJSON in `.sdo-runtime/controller-evidence/` on the repository volume before the state CAS. The state ConfigMap holds a SHA-256 annotation and omits program histories, tool calls and task bytes from both the incident and pending closure; loading verifies the digest and restores the full records before closure or outcome derivation. The digest participates in no-op and ambiguous-write identity checks. Archives survive proposal cleanup and remain available for offline evidence analysis. Every controller-written ConfigMap document (state, failed-closure archive) is held to a 768 KiB budget and fails with a typed error naming its largest fields instead of an API 422. A closure never carries the frozen task, and a reflection request carries each incident's program runs, tool calls and plan applications once, in the outcome, and none for the latest two prior outcomes. A retry reuses those bytes for the request ConfigMap and result validation. For state written before this field existed, the controller validates and adopts the already created request ConfigMap. Settle observations remain in the live incident view and `sdo status`; they do not replace the worker's dispatched task. Every worker task travels in one request ConfigMap and is held to 512 KiB (the API server rejects ConfigMaps near 1 MiB); there is no second transport. Before a responder or reflection task is frozen, Go degrades an over-limit request in a fixed order and records each step in the request's `compaction`: prior-incident history, then memory the incident does not mention, then memory documents, then raw program runs and tool calls (the outcome keeps the full record). A reflection request that still does not fit is skipped with the typed, non-retryable reason `request_too_large` in the broker ledger, and the incident closes on health alone; a responder request that still does not fit fails before any Job with a typed error naming its largest components. A task frozen by an earlier build that is over the limit is discarded and rebuilt. Every agent worker kind gets its memory request and limit from one constant pair (`WorkerMemoryRequest`, `WorkerMemoryLimit` in `worker_registry`). A worker container killed for memory ends its reflection with the typed reason `worker_oom_killed`, or `worker_resources_rewritten` when its pod was admitted with another limit than the Job asked for (a cluster admission webhook rewrote it, so resizing does not help); both are recorded in the broker ledger like `request_too_large`, and the incident closes on health. The failure text names the requested and admitted limits. Workers write one curated line (error class and short message) to `/dev/termination-log` on a startup or run failure and the controller reads only that, never logs. A permanently failed closure stays with its own incident: the benchmark adapter records it as that incident's rejected receipt and the next fault proceeds.

The controller keeps a baseline of the application namespace's configuration, taken from namespace-scoped informers over Services, workloads, NetworkPolicies, ConfigMaps, Secrets, Roles and RoleBindings. The first healthy evaluation sets it. A later healthy snapshot replaces it only after a two-minute settle period with no unhealthy evaluation, and a maintenance pause resets it. When an incident opens, the request carries `state_changes`: the objects added, removed or modified since that baseline, with field-level before and after values. ConfigMap data appears only as digests. Secrets use strict metadata-only (`PartialObjectMetadata`) list/watch requests with no full-object fallback: the baseline stores name, UID and resourceVersion, and content changes appear as resourceVersion changes. Secret type and key names are unavailable from that API, so they are omitted. Secret annotations and unrelated labels are discarded before caching. Unsupported or forbidden metadata reads leave Secrets unobserved. The controller retains only Secret list/watch RBAC, because metadata negotiation uses those same verbs; Kubernetes RBAC cannot restrict a grant to metadata representations. Kinds the controller cannot list are reported as unobserved. Objects that existed unchanged while the application was healthy never appear. The diff is computed in memory from the informer cache, so it adds no API calls on the dispatch path.

Responder Jobs receive the prober's Service address (`http://sdo-prober.<control-namespace>.svc:8080`) as `SDO_PROBER_URL` when they are dispatched, so a prober pod replaced mid-incident stays reachable. `python3 -m sdo.agents.responder.status_cli` runs one verify burst of the incident's scenarios through the prober. It exits 0 when healthy, 1 when unhealthy and 3 when the prober is unavailable, and only scenarios the steady probe has seen succeed can make it unhealthy. The controller also publishes an `incident_view` in its `sdo-controller-state` ConfigMap while an incident is open (the health-detector findings that still block closure and the current state diff) and passes its location as `SDO_CONTROLLER_STATE`; the status is then also unhealthy while a non-traffic health detector fires, and it lists state changes made after the request was taken. The gate has its own clear hysteresis, separate from closure's: while a blocking finding is clearing, the controller re-evaluates its detector every second for the gate only, and moves it to the view's `clearing_findings` after 3 consecutive clear evaluations spanning at least 2 s. Those re-evaluations never reach the finding tracker or detector history, so closure still waits for the health judge's own clear persistence. Responders must run it before declaring a repair. Each confirmed root cause cites structured live `evidence` (a detector finding, synthetic traffic, a state change or a live observation) and the `explained_detectors` it accounts for. Scripts and manifests go in `static_context`, which is never evidence. Closure derives one of the three outcome classes from responder completion, independent health clearance and recorded repair mutations. It does not parse evidence references or attribute each claimed cause. Root causes in successful prior outcomes remain responder claims. Strict receipts retain the independent health-clear requirement and distinguish `sdo_mitigated` from `cleared_without_sdo_action`; only successful outcomes are eligible for reflection. Responders label any helper pods or Jobs they create `sdo.dev/responder-helper=true`. When the responder completes, the controller deletes them before evaluating the closure gate and records them in the closure as `cleaned_helpers`. A verified success commits only after health has stayed clear for the closure stability window (15 s) on every closure path; a relapse inside the window resets it, and `health_cleared_at` still records when health first cleared. If health does not clear or admitted source drift remains unresolved within the verification window, the controller closes the incident once with no final detector states; the outcome is `failed`, reflection does not learn from it and the strict receipt rejects it. A split-namespace install grants this through a narrow pods/jobs delete Role.

Before dispatch, the controller deterministically selects up to three successful prior outcomes using exact finding fingerprints and, secondarily, detector/rule/resource-kind compatibility. Only compact root-cause, repair, playbook, and source-compatibility evidence enters the incident request. The responder treats that evidence as a hypothesis and must confirm, adapt, or reject it against live state; historical actions are never replayed automatically.

After independent health verification, the controller records a versioned per-incident outcome. Reflection receives the closure, current and prior outcomes, and an accepted-commit-bound logical memory snapshot. It starts in a fresh session by default or resumes the responder session when configured with a valid session ID. The versioned `outcome-reflection-direct-v1` capability edits `.sdo/` directly and runs `sdo-operator memory check`, fixing named diagnostics in the same checkout until green within its turn budget. Its result contains `updated` and affected logical playbook IDs, or `no_change`/`sdk_gap` with concrete reasons and no diff. Deprecated artifact payloads are rejected. Dispatch pins `SDO_ACTOR=responder`, `SDO_PHASE=reflection`, and `SDO_BASE_COMMIT`; the worker image contains the CLI, Go toolchain and operator sources. Go checks decision/diff consistency, then the broker independently repeats `validate.Check` against the candidate tree before publishing and triggering a controller rollout. The durable broker ledger keeps `reflection_rejections` (attempt, typed `MemoryDiagnostic` findings, and rejection timestamp) before releasing rejected files, and marks `reflection_rejected` after the retry bound; content failures permit two corrections (three dispatches total), while infrastructure errors do not consume that learning budget. The warm rule uses confirmed prior outcomes and logical playbook IDs; a verified repeated match may skip an unnecessary reflection turn when the broker can prove no new learning is required.

The controller keeps one one-shot standby responder per application after acquiring leadership, as an optional early claimant for a task the cold path would otherwise run in a new Job. The task record, location and contract are the cold path's; there is no warm-specific task. The production Go daemon starts the registered command unchanged; the Python entrypoint imports its worker and blocks on private inherited pipes without reading incident data. A fresh daemon readiness heartbeat permits an immediate claim through the existing narrow PVC filesystem transport: a create-only claim file in the pod's inbox, so exactly one claimant wins. The controller copies the canonical checkout and session into the pod's pre-mounted directories and swaps each pair with one atomic `renameat2(RENAME_EXCHANGE)`, so the pod holds the canonical paths; every intermediate state leaves a valid canonical directory and nothing needs rollback. It then publishes the existing typed AgentTask in the read-only inbox, and the daemon binds the accepted commit and the loopback API-history proxy before releasing the Python worker. A consumed slot is never reused; replacement begins at the claim. Absent readiness, active capture storage, a dead pod or any claim failure takes the cold Job path. A published claim always rejoins its original Job, preventing duplicate incident execution after an uncertain response. On controller start every standby whose task was never published is deleted and a fresh one created; a published one is an ordinary running responder. An idle pod has no Job deadline: its heartbeat expires after 30 seconds without a controller, and a claim never published expires after five minutes. Once claimed, the normal responder budget applies. Provider session homes retain their canonical incident paths for reflection. Service accounts, credentials, security contexts and read-only corpus/history/tool boundaries are shared with cold Jobs; no new inbound claim listener or API permission is introduced.

Only the controller image contains the Go compiler and Docker-layer-cached, trusted build-cache seed for the controller SDK and runtime dependencies. The production `operator/toolchain` edge seeds an immutable image-bound compiler directory on the repository PVC before the controller accepts incidents. Incident Jobs, and any other worker Job dispatched where that seed is complete (reflection, health refresh), mount only that compiler and cache read-only, avoiding per-Job copies. A worker Job whose dispatching process has no completed seed retains a private emptyDir populated by a controller-image init container; their runtime images contain only binaries and runtime sources. Compiler mounts are read-only, and the trusted init container has no candidate or provider-credential mounts. Each isolated validation copies that seed into its own ephemeral writable cache before compiling application-owned detectors. This preserves the read-only source, network isolation, and per-run scratch boundary while avoiding repeated cold dependency compilation.

## Controller-installation boundary

The Go `sdo/operator/install/` package installs the controller from the broker-accepted commit. The installer owns controller resources and repository synchronization. The SREGym adapter adds benchmark submission resources, readiness, `production-receipt/v2` evidence, and cleanup without changing controller semantics. Benchmark `recorded-actions` policy admits live-only repairs backed by structured action receipts; broker outcome and reflection acceptance still require independent verification.

By default the controller shares the application namespace. `install.Config.ControlNamespace` (and `sdo-operator host --control-namespace`) instead places the controller Job, repository PVC, credentials, state ConfigMap, Lease, maintenance ConfigMap, and responder and validator Jobs in a separate per-application namespace labelled `sdo.dev/controller-namespace=true`. The application namespace then receives only two namespace-scoped grants bound to that namespace's ServiceAccounts: a read-only observer Role for the controller and the responder's repair Role; no ClusterRole is created unless named webhook observation is explicitly provisioned in the installer ceiling. The repair Role is generic namespace-scoped edit on application resources (the shape of Kubernetes' `edit` role, without Secrets or RBAC objects, and with pods/exec, attach and port-forward for parity with the Codex baseline), not a list of particular repairs. Responder pods point kubectl at the incident's application namespace. The controller installer does not rewrite application charts or manifests. A long-running install runs the Go builder supervisor: an accepted detector or Go program source change requires a supervised controller rebuild and relaunch before acknowledgment; an unchanged controller stays up. The operator-owned `sdo-controller-maintenance` ConfigMap (`state: paused|active`, `generation`) pauses detector evaluation and application observation for planned redeploys while in-flight closure, reflection, and acknowledgement continue; resuming re-lists the application namespace with a fresh informer generation, logs `controller_maintenance` with the generation, and evaluates every detector once. With `reuse_existing`, re-running the installer keeps a running controller whose install fingerprint matches and only re-applies the namespace-scoped grants.

SREGym deletes and recreates the application namespace for every problem (and reconciles away unexpected namespaces and PersistentVolumes). The benchmark's opt-in persistent mode (`persistent_controller = true` under `agent_config.sdo_codex`) therefore installs one controller per application in `<application-namespace>-sdo`, asks the harness through `SREGYM_PRESERVE_NAMESPACE_LABEL` to keep that namespace and its volume, pauses the controller after each incident is verified, and on the next problem calls the Go installer's `inspect-existing` to restore application-namespace grants before resuming with a new generation. Matching the saved fingerprint and Pod UID does not bypass this access restoration. Fault injection requires the first all-clear evaluation after that resume; its five-minute readiness deadline reports the namespace, generation, and any forbidden informer observation, with progress logged every 30 seconds. Per-problem resolution ends at controller-verified health; reflection continues asynchronously. Before the next injection the adapter waits for the previous incident's acknowledgment (`reflection_drain`), syncs the controller's repository into the new workspace, and writes the previous problem's strict receipt; pipeline teardown drains the last incident, then delegates runtime shutdown to `sdo-operator stop`, retaining the repository PVC and accepted state.

`sdo-operator stop [--control-namespace <namespace>] [--kubeconfig <path>]` owns production shutdown. With no namespace it inventories all namespaces; with a namespace it touches only that scope. It stops controller/operate dispatchers before worker Jobs, Pods (including their proxy sidecars), prober Services and isolation policies. Inventory recognizes existing installer names and runtime labels, including worker Job template labels, so it works after the controller disappears. It waits for deletion up to `--timeout` and returns JSON `removed` and `leftovers` lists; errors exit nonzero. `--inspect` returns the same inventory without deleting. Namespace, RBAC, PVC, ConfigMap, Secret and Git contents are preserved. This command has no benchmark dependency.

The benchmark adapter's `persistent abort --state <path>` kills only registered same-user process identities, stops each recorded controller with its own kubeconfig without draining, removes benchmark bridge/registry workloads and the application namespace, and verifies runtime and helper absence. Cleanup targets are saved before lifecycle installation, retained for retries, and pending incidents become timestamped `aborted_incidents` in benchmark state. The launcher in `scripts/run_sregym.sh` traps SIGINT/SIGTERM and invokes abort. Its private process-group guardian kills host helpers and the runner when the launcher vanishes, including SIGKILL. Helper commands are supplied as foreground argv through `--helpers` or `SDO_RUN_HELPERS_FILE`; the registry stores PID, boot ID, start time, UID and group, never arguments or credentials. A stable abort locator points to the state in the result directory, preserving deferred receipt publication. The runner imports only the adapter's process-registry binding utility. Historical unregistered helper processes cannot be discovered or stopped by this registry.

The adapter reads controller settings from `[agent.sdo_codex]` in an experiment or `[defaults.agent_config.sdo_codex]` in a pipeline, exported by the runner as `SREGYM_EXPERIMENT_AGENT_CONFIG`. `deployed_manifest_paths` is an explicit list of non-empty repository-relative manifest file/directory paths (default `[]`); the hotel source adapter deploys `kubernetes`, so use `["kubernetes"]` for that application. A seeded lifecycle with no selection skips ownership adoption and cannot provide accepted-source drift checks or source repairs; the driver does not infer selection from seed provenance. Repeated `--deployed-manifest-path` CLI arguments replace the configured list. Set these before installing the controller; changing settings on an already running persistent controller requires a new installation.

Persistent benchmark experiments may additionally preserve shared cluster infrastructure behind an explicit SREGym-only flag. A durable in-cluster marker gates reuse, and the warm storage/observability installation becomes the reconciliation baseline; application and injected-fault state are still cleaned between attempts.

The installation code provides manifests and orchestration logic; the always-running runtime is `sdo/operator/runtime/` in Go. Unit and Go integration tests validate contracts; successful operation on a particular cluster still depends on cluster access, images, credentials, storage, and the target application's deployment artifacts.

## Shared libraries

- `libs/agent_cli/structured.py` runs every production agent turn through [agentshim](https://github.com/vic-lsh/agentshim). SDO's deployer, health judge, responder, and reflection backends name a provider (Codex or Claude Code) and an access level, and keep their subsystem-owned prompts and Pydantic validation. The access levels map onto each provider's own confinement: `read-only` is Codex's read-only sandbox, or Claude's settings sandbox denying workspace writes plus the Edit/Write tools; `workspace-write` is Codex's workspace-write sandbox (network off) with `sdo-operator memory ls|check` available for read-only memory inspection, or Claude's sandbox with the same commands available. Codex reads that rule only from `$CODEX_HOME/rules`, so each Codex workspace-write turn runs with a private, non-resumable `CODEX_HOME` under `$SDO_CODEX_HOME_ROOT` (default `~/.cache/sdo/codex-homes`, outside the workspace and `/tmp`) holding the rule and a 0600 copy of the user's `auth.json` (or none when `CODEX_API_KEY`/`OPENAI_API_KEY` is set), deleted when the turn ends; `danger-full-access` removes confinement for sessions that operate the cluster. Each turn also returns the shell commands the agent started, which the lifecycle audits for reads outside the application checkout; Codex omits commands its sandbox denied from that stream, which the audit tolerates because denied commands never ran. The worker session directory is retained for seven days after durable acknowledgment, pruned daily, and exported by `runtime-artifacts` under `sdo_runtime/sessions/<incident-hash>/{codex,claude}/`; verified reflection may resume the exact responder session when configured.
- SREGym-specific protocols and runner utilities live under `benchmarks/sregym/`, not `libs/`.

## Supported and excluded scope

Supported production scope is the lifecycle, memory, controller, responder, and Kubernetes runtime described above. SREGym code is supported as benchmark infrastructure.

Excluded from production scope are bounded shell-monitor loops, generated shell health checks, a second deployment lifecycle, standalone Compose fault injection, historical trajectory recorders, and benchmark verdict logic inside production packages.

See [testing-guide.md](testing-guide.md) for validation commands.

Status meanings:

- **Active** — selected by the public production command.
- **Benchmark-only** — lives behind the SREGym adapter and is excluded from production imports.

## Responder Kubernetes API history

Native responder Jobs start one Go recording proxy as a Kubernetes native sidecar
(requires native-sidecar support, Kubernetes 1.29+). It listens on plain HTTP at
`127.0.0.1:18080`; the generated application kubeconfig points there without
client credentials. The proxy uses the same Pod service account upstream, with
its normal TLS CA and rotating token, so existing responder RBAC still applies.
The native controller selects the existing validator image for the proxy binary,
independent of the registered agent's language or image. Startup probing gates
agent launch. No controller Service, TokenReview, serving certificate, or identity
mapping is needed: the controller passes the incident ID when launching the Job.

The memory edge prepares `.sdo-runtime/sessions/<digest>/api-history/` on the
repository PVC. Only the sidecar mounts that directory writable; responder and
fresh/resumed reflection mount it read-only. The sidecar gets no model-provider
credential mounts or environment. It appends request start/end envelopes with
UTC times, verb, GVR, namespace, name, subresource, status and opaque
resourceVersion. Sanitized response bodies are SHA-256-addressed and deduplicated.
The proxy uses its own strict projection, independent of controller snapshots.
Its positive per-kind allowlist retains Service routing ports and type; workload
replicas, rollout type, container images/ports/probe shapes/resource budgets and
reference names; Pod readiness, restart/state reasons and exit codes; endpoint
address counts and ports; Event reason/type/object/count/timestamps; and reviewed
HPA, PDB and NetworkPolicy spec structure. Labels and selector match labels retain
keys with empty values; selector expressions retain keys/operators, never values.
Service clusterIP and Pod podIP become presence booleans; endpoint addresses become
ready/not-ready counts. Secret/ConfigMap payloads remain key names only. All annotation contents, environment values, commands/arguments (including probe exec), probe
headers/hosts and all HTTP probe paths (including pathname segments, queries and
fragments), mount paths and messages stay opaque under an explicit `redacted`
marker. Probe handler kind, port, scheme and timing remain visible. Image strings
are parsed and normalized as named container references; invalid references, URL
schemes, userinfo, queries and fragments are omitted, while valid repository, tag
and digest identifiers remain visible. Conditions, current/previous container
states and Events retain only the finite reason vocabulary in
[projection_leaves.go](../sdo/operator/apihistory/projection_leaves.go); other string
reasons become `redacted`. Closed enums, quantities, CIDRs, named ports, timestamps
and downward-API expressions are validated, with canonical quantities, CIDRs and
UTC timestamps. Bulk env references expose name/optional, never a key selector.
Identity names, label/selector keys, reference names/keys and valid image
identifiers are an accepted disclosure boundary: user-chosen identifiers can
encode secrets, so this is not a universal credential-absence guarantee. These
rules apply to new recordings; existing blobs retain their original projection.
The identity policy is centralized in
[identifiers.go](../sdo/operator/apihistory/identifiers.go), with these per-field grammars:

| Retained field | Disclosure grammar |
| --- | --- |
| Object names (except Service), node names and reference names | DNS-1123 subdomain, at most 253 characters |
| Service object names | DNS-1035 label, at most 63 characters |
| Namespaces, container/status/HPA-container names, volumes and mount names | DNS-1123 label, at most 63 characters |
| Label keys, selector keys and referenced downward-API annotation/label keys | Kubernetes qualified name: optional DNS subdomain prefix and a 63-character name |
| Secret/ConfigMap data keys and env key-selector keys | Kubernetes ConfigMap key: alphanumeric, `.`, `_`, `-`, at most 253 characters; reserved dot paths excluded |
| Environment variable names and reference kinds | C identifier (`[A-Za-z_][A-Za-z0-9_]*`), a conservative disclosure policy |
| Container port names, string targetPorts, network-policy and probe ports | Kubernetes named port: lowercase alphanumeric/hyphens, at most 15 characters, with a letter |
| Service and endpoint port names | DNS-1123 label, at most 63 characters |
| Reference apiVersion | DNS-1035 version label, optionally preceded by a DNS-1123 group subdomain and `/` |
| HPA Pods/Object/External metric names and Resource/ContainerResource resource names | Kubernetes qualified name; custom names are retained only in that grammar, even if API admission allows broader text |
| Pod condition types, including custom readiness conditions | Kubernetes qualified name |
| Workload condition types | Finite reviewed vocabulary: `Available`, `Progressing`, `ReplicaFailure`; all other strings become `redacted` |
| Request group; version; resource/subresource; namespace; name | DNS-1123 subdomain; DNS-1035 label; DNS-1123 label; DNS-1123 label; DNS-1123 subdomain, respectively |
| Object/list/item and envelope resourceVersion | Nonempty ASCII decimal digits; other opaque values become tokens |

Invalid identity leaves and map keys are omitted without echoing input or validator
errors. HPA `scaleTargetRef` and object-metric `describedObject` use the actual
CrossVersionObjectReference shape (apiVersion/kind/name), with no namespace.
Invalid request names/subresources and nondecimal resource versions instead become
`opaque:<HMAC-SHA256>` tokens. The hash uses a random 32-byte key stored privately
in that history's `identity.key` (0600), with a field domain separator. Equality
is preserved within each field across objects, lists, envelopes and recorder
restarts in one history, without correlating separate histories. Known
subresources such as `status`, `scale`, `log`, `exec`, `attach`, `portforward`,
`token` and `finalize` pass through. Other valid subresource identifiers also pass;
invalid group/version/resource/namespace segments are omitted. Altered request
identity marks the envelope partial so an invalid namespace cannot widen an
absence claim. The key and raw rejected values never appear in evidence CLI
output. Numeric resource versions are retained as identities, not interpreted as
ordered counters. A valid referenced annotation key remains visible in a
validated downward-API expression; its annotation value remains opaque.

Unknown fields and wrong JSON shapes cannot widen the allowlist. The
enforced field-by-field allowlist is in [projection.go](../sdo/operator/apihistory/projection.go). Unsupported representations,
all log bodies, watches and connection payloads retain envelopes only. Body
capture requires declared `application/json` (optionally UTF-8 charset) and absent
or identity `Content-Encoding`; other declarations remain metadata-only even if
the bytes look like JSON. Forwarded status, headers and bytes are unchanged. Headers
and request bodies are never recorded; the reverse proxy discards upstream error
logger text, including body-copy errors.

The default incident budget is 64 MiB / 10,000 envelopes, with a recording queue
bounded by 1,024 entries / 8 MiB of already-projected envelopes and bodies.
After forwarding returns, up to eight transient workers project responses no
larger than 256 KiB. Larger bodies or saturated projection workers retain
metadata and a marked gap; raw bytes never enter the recording queue or storage.
A reserved manifest reports completeness, dropped counts, bytes, writer status
and restart epoch. Unfinished requests and restarted writers invalidate
continuity. Every initialization attempt first leaves a recording-disabled
marker until successful open. A directory lock exposes the live forwarding
generation to read-only readers even when recording writes fail and a previous
closed manifest survives. Disk, queue and cap failures leave forwarding active
and mark gaps; unavailable storage is Unknown. Upstream failure returns a
Kubernetes-shaped 502; write connections are fresh and never automatically
retried. Every response and stream reaches the caller unchanged. History follows
existing incident-session cleanup, with no additional retention policy, and adds
no durable `.sdo/` artifact class.

Reflection can run `sdo-operator evidence history`, `show --event N`, and
`state Kind/namespace/name --at RFC3339|before-first-mutation|after-fix` using
`SDO_API_HISTORY_DIR`. These commands read files only. State is explicitly last
observed, stale and Unknown at the requested instant because metadata-only
watches provide no continuity. Complete unfiltered scoped lists or named GET
404s can observe absence only at their observation times. Lookups match an
explicit GroupResource; `Kind.group/namespace/name` can qualify built-in kinds,
and unsupported groups stay Unknown. Cluster-wide lists retain positive matches
using item namespaces, with no inference of broader absence. The first mutation
boundary includes opaque exec/attach/port-forward regardless of query parameters
and excludes only recognized supported dry-run writes; crossing requests,
partial history and filtered/paginated lists cannot establish broader facts.
The controller writes `recovery.json` only for independently verified successful
or cleared closures; `after-fix` requires compatible observations beginning after
that marker. A final API write is never recovery evidence. Proxy history does
not replace captures, pre-apply checkpoints or action receipts, and is not a
replay source, coverage policy or `no_recorded_repair` implementation. Direct network
traffic, alternate kubeconfigs and controller SDK reads remain outside its view.

## Component map

The Go responder loop owns convenience-menu execution through the ordinary PlanExecutor; the Python
CLI submits options and parameters and receives reports in the same session. Existing controller RBAC
and executor checks govern execution; the benchmark adapter owns strict experiment receipts.

| Capability | Repository implementation | Status | Evidence and limit |
|---|---|---|---|
| Public lifecycle | `sdo/operator/operatorcli/`, `lifecycle/`, `deployment/`, `artifacts/` | Active | Go host and in-cluster operate coordinate sequential role-owned proposals with trusted deployment and health evidence. |
| Lifecycle decisions | `sdo/operator/lifecycle/`, `artifacts/`, `deployment/` | Active | Source receipt binds the candidate tree, observed immutable images, and selected manifest paths; architecture cites the broker-accepted source commit. The broker carries the paths through accepted source recovery into the controller launch. |
| Modular agent registration | `sdo/contracts/proto/.../worker.proto`, `sdo/operator/worker_registry/`, `sdo/agents/registrations/` | Active | Strict ProtoJSON capabilities and typed task/result URLs select workers without assuming Python or a model provider; Kubernetes Jobs mount isolated proposal checkouts. |
| Responder API observations | `sdo/operator/apihistory/`, `kubeproxy/`, `operatorcli/evidence.go`, `worker_registry/` | Stage 1 | Per-task localhost sidecar uses Pod RBAC; bounded projected runtime history and read-only CLI expose last-observed state and Unknown without replay or coverage decisions. |
| Built-in agents | `sdo/agents/{deployer,health_judge,responder}/` | Active | Each implementation owns its prompts and provider choice; tasks carry logical memory snapshots; reflection and the judge return decisions/IDs after direct file edits. |
| Code-path composition | `sdo/operator/runtime/code_path*.go`, `sdo/operator/shellpath/` | Active, shell opt-in | Named plugin sets select detector source, coverage, ordered PIPE stages, memory validation, activation and role guidance. Go remains the default; shell uses a long-lived read-only runner and shared incident lifecycle. |
| Detector SDK and execution | `sdo/operator/sdk/`, `core/`, `runtime/` | Active | Deterministic Go snapshots with shared cached/recorded detector and Diagnose reads, detector evaluation, findings, batching, leader election, and generated controller state have Go tests; lifecycle storage is private to `runtime/internal/lifecycleio`. |
| Program coverage and responder loop | `sdo/operator/runtime/`, `sdo/agents/responder/tools_cli.py`, `sdo/contracts/proto/.../program.proto` | Active | The controller diagnoses bound candidates, serves incident-scoped `sdo` tools through the responder session PVC, including rendered accepted source, applies each plan as written, reports the resulting verification, and records tool and program evidence on the outcome. `apply` has no in-command judging: no dry-run after-images, owned-field or concurrent-change checks, write retries, or automatic rollback after a landed write; a write conflict is returned as the error, a failed rollout or bound verification leaves the plan applied and recorded (with a revision-bound `application_verification_failure` signal); a different explicit open finding does not invalidate a cleared bound diagnosis, and the responder decides whether to call `rollback` or retry. Only a failed multi-write plan compensates the writes that already landed. `sdo act --resource Kind/ns/name -- <command>` runs one live mutation in the responder session and records its repair receipt (resources, timing, outcome and command line) under the session scratch directory; the responder worker merges those receipts into `IncidentResult.repair_actions`, so the model writes no receipt or wrapper script. Configured response stages select mutations through the same executor. Accepted single-entry repairs can run deterministically before the responder; typed stage reports retain per-finding results. Failed verification retains completed applications, and the responder decides explicit rollback. Prepared incident tasks preserve the original coverage view across restarts. |
| Drift change feed and response | `sdo/operator/deployment/`, `sdo/operator/sdk/drift.go`, `sdo/operator/runtime/`, `sdo/contracts/proto/.../messages.proto`, `sdo/agents/responder/` | Active | SSA adoption records source contradictions without repairing them. Informers admit changed declared fields and source-absent application/control-plane objects against cached accepted deployment state. Detectors supply findings; finding-bound playbook diagnoses populate the coverage view for responder-selected mitigation. Playbooks never generate `drift.<program>` findings. Unexplained observations open ordinary latent `drift.unexplained` incidents after bounded batching, or attach to the open incident and its late-findings view. The responder establishes source contradiction and a violated relation or observed impact before mutation. Verification and closure recheck retained accepted-source drift as well as detector health. Drift-only batches coalesce for 20 s / 8 events and are otherwise admitted like health findings; there is no dispatch or token rate limit. Always-on incident detectors cover faults; playbooks have no watch-event or timer-driven diagnosis. |
| Synthetic traffic | `sdo/operator/sdk/traffic/`, `runtime/prober/`, `builder/`, `.sdo/diagnostics/traffic/` | Active | Generated workloads run in an isolated prober; acceptance remains independently validated. |
| Metrics input | `sdo/operator/sdk/metrics/`, `runtime/prometheus/`, `runtime/metrics_observer.go`, `memory/generic_health.go` | Active, opt-in | Frozen per-cycle Prometheus results for threshold and alert detectors; installed only with a metrics URL, and an unavailable source is a detector error. |
| Offline closed-loop checker | `sdo/operator/runtime/replay/`, `builder/replay.go`, `cmd/sdo-replay/` | Active | The production builder links accepted detector/program registrations into an offline driver. Production PlanExecutor writes client-go fakes seeded from CorpusCase; explicit manifests supply accepted source. JSONL separates API effects, simulated controller reactions, refused effects and false positives. It does not create live trust credits or write operational memory. |
| Controller builder and supervisor | `sdo/operator/builder/` | Active | Go builds and validates accepted diagnostics and supervises controller rollout with durable provenance. |
| Isolated detector validation | `sdo/operator/memory/validate/`, `builder/`, `operatorcli/`, `broker/` | Active | `memory/validate.Check` is the shared acceptance pipeline; it always runs structural, semantic and ownership checks, then compile/vet/tests using `CompileCandidate` locally or in the broker's isolated Job. Agents and benchmarks reach it through `sdo-operator memory check|import|ls|new|outcomes`; `check` requires `SDO_ACTOR`, `SDO_PHASE` and `SDO_BASE_COMMIT`; `check --json` emits a `MemoryCheckReport` of `MemoryDiagnostic` messages. |
| Standby responder transport | `sdo/operator/worker_registry/standby*.go`, `operatorcli/standby.go`, `sdo/agents/responder/standby.py` | Active | One incident-free preloaded worker; create-only single-use claim, atomic directory exchange onto the canonical paths, exact accepted revision; immediate cold fallback. |
| Compiler distribution | `sdo/operator/toolchain/`, `sdo/operator/cmd/sdo-go-cache/`, `Dockerfile.runtime`, `scripts/sdo-images.hcl` | Active | Controller startup seeds image-bound read-only compiler mounts for every worker it dispatches; a worker without a completed seed falls back to a private init-container copy. Validators use immutable compiler/module/cache image layers and a read-through Go cache helper, writing only new build entries to private scratch; no validation or ownership bypass. |
| Commit authority | `sdo/operator/broker/` | Active | The Go broker serializes sharing controllers, replays stale diffs, validates the merged tree, returns typed conflicts, and alone publishes role-owned changes; verified source publishing is bound to a trusted candidate-tree digest and the deployer receipt. |
| Logical operational memory | `sdo/contracts/proto/.../outcome.proto`, `sdo/operator/memory/`, `outcome_derivation/` | Active | Go supplies immutable proposal-commit-bound logical catalogs across concurrent publications, validates direct reflection and judge edits, prepares generic health policy detectors, and stores typed outcomes as versioned per-incident JSON documents. |
| Outcome reflection and reuse | `sdo/agents/responder/reflection_worker.py`, `sdo/operator/runtime/native_broker.go` | Active | Go persists one per-cause eligibility result before committing each successful outcome. Exact positive and fixed-negative cases use deterministic recording; questions and rejected cases use the in-session reflection worker. Both paths use the same proposal checkout, independent validator and broker publication; repeat recording adds no README note. |
| Namespace onboarding | `sdo/operator/onboarding/`, `operatorcli/onboarding.go`, `runtime/source_onboarding.go` | Active | Create-once acknowledgement of install-time identities after a confirmed full in-cluster detector sweep, including event-driven partial schedules, available rendered inventory and admissible health/render lineage; accepted-source dependencies and pre-existing identities are distinct from incident findings and learning evidence. |
| Controller installation and stop | `sdo/operator/install/`, `operatorcli/stop.go` | Active | Go installs the accepted checkout and stops only owned runtime resources, including orphan workers; accepted state survives shutdown; installer-owned observation policy bounds controller/bootstrap read grants. |
| Benchmark abort and helper lifetime | `benchmarks/sregym/adapter/{persistent,launcher,processes}.py`, `runner/runner.py` | Benchmark-only | Pre-install cleanup targets, per-controller kubeconfigs, timestamped aborted incidents and process identities support no-drain abort; the runner binds its result-state path to the launcher registry. |
| Scoped observation and replay | `sdk/`, `core/`, `runtime/`, `memory/observation/` | Active | Accepted-commit-bound safe projections, per-target availability, qualified drift and versioned captures; no metrics dependency. |
| Toy cluster validation | `apps/toy/`, `benchmarks/sregym/fastloop/toy.py` | Test-only | A three-service kind application with local readiness, real endpoints and watches, scripted workers and cluster-state oracles. Pinned Calico enforces NetworkPolicy, including the production validator isolation canary. |
| SREGym adapter and receipts | `benchmarks/sregym/`, `third_party/sregym/` | Benchmark-only | Benchmark submission, the Go submission guard and strict-receipt checker, and harness verdicts never enter production packages. |

The toy real-cluster validation tier keeps reusable application and fault code in
`apps/toy/`; fastloop selects it through its worker interface without the external
SREGym harness. It uses the same production controller, broker and memory edge.

Operator-authored incident memory uses `sdo-operator memory import prepare --root APP --worktree-root BROKER --id IMPORT_ID`, followed by edits in the returned isolated checkout and `memory import publish` with the isolated validator Job configuration (see `memory help`). The `human/import` grant includes incident detectors, incident playbooks/index and libraries plus their operator corpus; it excludes health detectors, the health-objective playbook, source, goals, outcomes and lifecycle provenance. Imported cases do not claim capture digests or incident provenance. Every operator positive case runs the production offline closed loop in addition to normal compiler tests on all later builds. Offline success supplies no live trust or outcome; responders receive imported programs as untrusted until ordinary verified live evidence qualifies them.

General SDK actions (`PatchObject`, `ApplyObject`, `KubernetesRequest`, `EnsureExec`,
`EnsureHTTP`, `RunJob`, `WaitFor`) use the production executor with API RBAC and
Secret-data exclusion. General object operations are discovery-backed across
namespaced and cluster resources; legacy field actions retain their original
scope checks. External replay requires exact measured transitions, not Fixed
answers. See [general action contracts](../sdo/skills/sdo-playbook-authoring/references/general-actions.md).
The broker may append typed reflection SDK gaps to an existing outcome through
`RecordSDKGaps`; its original classification, verification and repair evidence
remain immutable. Only the memory storage edge handles the physical rewrite.

The native installer freezes out-of-source identities and the accepted commit in immutable control-namespace ConfigMap `sdo-namespace-onboarding-pending`, without running a host probe or waiting for health. Startup inventory preparation renders accepted source directly, including while onboarding is pending. Inventory readiness, health authority or confirmed observations never consumes an attempt. ACTIVE maintenance generation updates preserve running probes, baseline and queued changes. A real pause/resume starts a new confirmation sampling deadline, retaining frozen install identities and every prior invalidation, target configuration and observed serving port; pre-resume samples cannot authorize completion. Verified memory-only publications preserve the frozen install boundary. Unconfirmed probe windows are recorded as pending and cannot establish health. When confirmation windows are ready, the controller schedules one full detector sweep on the current snapshot even if ordinary event batches are partial, preserving every ordinary scheduler deadline. The original confirmation deadline still rejects late clear samples; observed commit or inventory contradictions invalidate the pending window. Immutable control records `sdo-namespace-onboarding-window` and `sdo-namespace-onboarding-rejected` prevent restart from forgetting sampling or rejection. An interrupted sampling window fails closed without recapture; ordinary fault detection continues. Rejection or expiry does not claim an unready evaluation. Before this confirmed complete evaluation the in-cluster controller atomically creates `sdo-namespace-onboarding-attempt`; only an error-free clear evaluation at the unchanged commit, outside any incident, publishes immutable `sdo-namespace-onboarding`. Failure or a crash leaves the snapshot pending without authority; restart and later healthy settling cannot retry or expand it. The same final ConfigMap atomically stores controller-owned `reachability-reference.json`: declared TCP ports consistently refused throughout the independently healthy install window become typed `declared-but-never-served` observations, not incident findings. A previously serving port still uses the unchanged failure confirmation gate. Target configuration changes or a subsequent successful dial invalidate an exclusion. Controller logs report install-to-record duration; three default 10-second probe rounds add at most two ordinary observation intervals plus probe and scheduler time. The remaining host health verifier checks cluster Service DNS before starting its local prober and returns `probe transport unavailable` when that transport is absent. The acknowledgement carries the accepted commit and time, survives controller state restore, and appears separately in incident and reflection inputs. Source manifest references declare dependency identities with typed unverified-content gaps. Later unknown objects still block closure; onboarding never grants content, repair or learning authority.

Explicit memory adoption is compile-only and does not establish health provenance. Matching inherited broker-validated judge inputs avoid refresh entirely. Otherwise startup refresh establishes authority before onboarding can claim its evaluation; a continuous single-parent chain of validated health-judge publications or changes confined to controller-owned `.sdo/rendered/`, with unchanged application inputs and human objective preserves the original install snapshot and commit. Source changes, objective changes, merges and unrelated publications cannot use this exception. The frozen onboarding ConfigMaps are never rewritten to a refreshed commit.


## Artifact ownership matrix

Menu options and parameters are agent tool requests. Sessions and reports are presentation state;
ordinary incident counters use the existing tools-state CAS document outside `.sdo/`.
Only the broker publishes derived incident outcomes. Controller-owned `prior_stages` records retain automatic and agent-stage evidence; agents cannot author their own prior-stage receipts.

| Artifact | May author | May update | Validation rule |
|---|---|---|---|
| `apps/toy/seed/.sdo/` | Test fixture author | Test fixture author | Hand-authored goal, architecture and health diagnostics; no model lifecycle attestation. Once installed, normal production broker ownership governs accepted changes and outcomes. |
| Accepted branch and Git common-dir broker lock (outside `.sdo/`) | Go broker | Go broker | Sharing controllers hold one lock through complete merged-tree validation and fast-forward publication; peer accepted learning survives every successful publish. |
| Shell health scripts: `.sdo/diagnostics/detectors/health/<id>/*.sh` | Health judge | Health judge | Shell mode: executable text, size/credential/path hygiene and broker role ownership; no Go compile or replay. |
| Shell incident scripts and notes: `.sdo/diagnostics/detectors/incidents/<id>/*.sh`, `.sdo/playbooks/<id>/{*.md,*.sh}` | Responder | Responder during reflection | Same broker grants; notes have no front matter or required structure. Outcomes remain controller-owned. |
| `.sdo/goal.md` | Human | Human | Agent proposals cannot change the objective. |
| `.sdo/arch.md` | Deployer after accepted source | Deployer or authorized upkeep | `generated_at_commit` must name the broker-accepted source commit and topology must match source; optional observation scope cannot exceed the installer ceiling or be changed by responders. |
| `.sdo/corpus/` | Responder or Go verified-repeat materialization; human operator in `import` | New cases through reflection or human import broker | Captures keep provisioned bytes/digests. Hand-written import cases require `operator.json` (`schema_version: 1`, `origin: operator`, description), positive labels, and separate `accepted.json` source; production `runtime/replay` must close every imported program loop. Accepted cases remain immutable. |
| `.sdo/lib/` | Responder; human in `import` | Responder drafts during incidents and publishes through validated reflection; human in `import` | Authored import surface, shared-library usage and executable validation remain mandatory. |
| `.sdo/playbooks/` | Health judge creates only its named objective; responder or human import authors incident playbooks | Responder drafts during incidents and publishes through validated reflection; human in `import` | Human import excludes `health-objective`; the same front matter, semantic, compile/vet/test and secret rules apply. Human/import commit trailers plus `SDO-Origin: operator` record authorship, without an outcome or learning credit. |
| Private script application ConfigMap and Job | Controller and native script runner | The original application reservation/Job only | Durable application ID plus Job UID; no retries or replacement of uncertain execution. API receipts, raw output and pre/postimages remain outside operational memory; outcome reports retain references and selected scope separately from measured writes. |
| Private standby inbox and claim file | Controller | One assigned worker reads its own task; only its readiness leaf is writable | Runtime transport only; accepted state and other incident directories are never mounted. |
| Image-bound compiler seed / private toolchain emptyDir | Trusted controller-image init container | Read-only worker consumers | Ephemeral build dependency; outside operational memory, with no candidate or provider credentials in the init container. |
| Validator compiler and Go cache | Pinned validator image build | Read-only image seed; private scratch delta through `sdo-go-cache` | No per-Pod compiler/module/cache copy; module downloads remain disabled and cache hits never write image state. |
| `.sdo/diagnostics/detectors/health/` | Health judge writes objective detectors; Go prepares generic service/traffic policy detectors, plus `metrics-alerts` only when a metrics URL is configured | Health judge lifecycle and shared Go preparation | Direct checkout edits and generated policy files pass the same check, isolated compile, and tests; health acceptance is independent. |
| `.sdo/diagnostics/detectors/incidents/` | Responder; human in `import` | Responder; human in `import` | Incident provenance, compile, matching, and near-miss tests are required. |
| `.sdo/diagnostics/traffic/generators/`, `traffic/workloads/` | Health judge | Health judge lifecycle | Workloads and generators obey engine limits and isolated validation. |
| `.sdo/diagnostics/traffic/generators/incident/`, `traffic/workloads/incident-*.yaml` | Responder | Responder | Incident-scoped traffic only; health acceptance remains judge-owned. |
| Outcomes: `.sdo/outcomes/<sha256(incident_id)>.json` | Controller through broker | Controller through broker | `OutcomeRecord` is the logical schema; classification is `success`, `cleared_without_action`, `unconfirmed` or `failed`, without diagnosis verification; versioned per-incident documents include controller-recorded tool calls, program runs, applications, loop counts, failure signals, and bounded drift events with responder verdict, reason and evidence using the legacy `investigation_*` field spelling; checked benign labels suppress only identical manager and current/source field observations, plus incident-open `playbook_coverage`, responder-selected program applications, verification, and explicit rollback. Base incident evidence is immutable; the broker alone may append typed reflection SDK gaps. The document is joined to closure phase telemetry in the broker ledger by incident ID and outcome commit. The storage edge imports retired JSONL streams; no agent has writer capability. |
| `.sdo/diagnostics/detectors/<class>/<id>/manifest.yaml` | Owner of the detector class | Owner of the detector class | One manifest per detector; its class, ID, and package must match its location. |
| `.sdo/schema-version`, `.sdo/lifecycle-provenance.yaml` | Upkeep or controller metadata path | Same owning path | Validation metadata, not additional artifact classes. Upkeep alone may migrate schema versions 1 and 2 to 3: it rewrites detector manifests, moves packages, and rewrites playbook paths in detector source to logical IDs, and the broker rejects any other detector change in the same commit. |
| Offline replay reports (outside `.sdo/`) | Local checker caller | Local checker caller | Offline analysis only; these reports do not authorize program trust or benchmark submission. The broker separately executes repair replay during publication. Accepted manifests remain separate fixture inputs; Fixed never supplies post-state. |
| Benchmark strict/rejected receipts (outside `.sdo/`) | Benchmark Go checker derives and accepts/rejects evidence | Python adapter transports and persists generated results | `NativeProductionReceipt` and `ReceiptFailure` define the decision; rejected raw evidence grants no memory or mitigation authority. |
| Benchmark fault descriptions and terminal infrastructure rows (outside `.sdo/`) | Harness subprocess and benchmark runner | Benchmark runner/analysis | Versioned descriptions preserve ordering and probe capabilities; unsupported repair/snapshot scope is rejected; infrastructure-only rows grant no agent credit. |
| Benchmark submission decisions (outside `.sdo/`) | Benchmark Go guard | Python only transports the decision and opaque upstream reply | Generated `SubmissionGuardDecision`; only the matching observed phase authorizes POST; completion polls never authorize POST. |
| Session API history (outside `.sdo/`) | Go proxy; memory edge writes attempt generation and verified recovery marker | Same controller-owned paths | Per-incident bounded append-only envelopes and projected content hashes; workers mount read-only; caps, gaps, unfinished requests, mismatched attempt generations and epochs cannot establish continuity. |
| Benchmark abort state and process registry (outside `.sdo/`) | Benchmark runner/adapter and launcher | Benchmark runner/adapter and launcher | Cleanup targets precede install; abort records pending incidents as aborted without claiming verified mitigation or writing operational memory. Process identities authorize signals only for the recorded UID, boot and start time. |
| Control-namespace `sdo-namespace-onboarding` ConfigMap | In-cluster controller after the sole confirmed full install sweep, using installer-frozen pending identities | Immutable; never expanded by incidents, restart or healthy settling | Accepted commit, establishment time and non-secret kind/namespace/name identities plus UID/configuration/ownership digests; generated descendants inherit only from a matching observed parent incarnation. Controller state carries a copy; the ConfigMap also owns immutable measured reachability-reference evidence. Separate immutable window/rejection records prevent interrupted sampling from granting later authority. |
| Runtime-state ConfigMap (outside `.sdo/`) | Go controller | Go controller | `ControllerState` is publication-validated by its storage edge; `ControllerObservation` is a read-only Go interpretation, never a second writer or persistence schema. |


Source inventory drift compares top-level non-secret informer objects against a cached accepted-deployment index. Source-absent namespace objects, RBAC bound to application ServiceAccounts and deletion-pending declared objects become latent `drift.unexplained` findings, including objects present on startup unless declared as accepted-source dependencies or present in the installer’s frozen environment inventory. This inventory records only pre-existing identities; it does not authorize health or reachability exclusions, hide declared source obligations, or accept objects first observed after installation. Namespace Roles/RoleBindings and bound ClusterRoles/ClusterRoleBindings are observed without Secret access; RBAC comparison includes rules, subjects and role references. SDO runtime objects, generated descendants of source objects and endpoint/event/status churn are excluded. The rolling healthy baseline remains diagnostic and cannot absorb these source contradictions. Both self-check and closure use the same in-memory inventory check with no added API or model call. Inventory renders accepted source directly. A restart before it loads makes a verified closure wait for a bounded time and never retires it; the unloaded inventory is noted on the outcome. Dependency-content gaps remain typed diagnostics in the read-only report (`evidence_gaps`); they do not become inventory drift or block presence checks. Remaining drift is information: the responder is shown it and the outcome records it. An exact observation-bound benign assessment with a reason and valid evidence is still recorded without repair or cause credit. ControllerState fields `accepted_source_drift` and `accepted_source_drift_revision` preserve checked acceptances across restart and memory-only commits, while application-source or object configuration/incarnation changes invalidate them. The generated controller exposes `--evaluate-once --source-drift-report /absolute/full.json` for read-only inspection; supply the accepted app root, namespace and explicit deployed manifest selection. Reset still cancels and awaits controller and worker Jobs before deleting captures or checkouts.

## Repository scope matrix

| Path | Scope decision |
|---|---|
| `sdo/agents/` | Production agent implementations with direct reflection and judge file authoring; contract types are generated protobuf messages; built-in agents may use Codex or Claude internally. |
| `sdo/contracts/proto/`, `sdo/contracts/_gen/` | Canonical language-neutral task/result and outcome schemas, with generated Python messages. |
| `sdo/operator/worker_registry/standby*.go`, `operatorcli/standby.go` | Production one-shot responder standby, private filesystem claim and daemon entrypoints; no benchmark imports. |
| `sdo/operator/toolchain/`, `sdo/operator/cmd/sdo-go-cache/`, `devmount/` | Production compiler/cache provisioning and opt-in development projections; benchmark file projections apply only to development worker Jobs, never controllers or validators. |
| `sdo/operator/worker_registry/`, `lifecycle/`, `deployment/`, `artifacts/`, `onboarding/` | Production Go interfaces and implementation. |
| `sdo/operator/broker/`, `memory/` (including `layout/` and `validate/`), `outcome_derivation/`, `install/` | Production Go authority with repository-scoped publication serialization, typed merge conflicts and immutable catalog reads; append-only reflection SDK-gap annotation, physical storage, deterministic verified-repeat corpus materialization and explicit generic-health preparation, the single operational-memory validator including human/operator imports with mandatory production offline replay, three-class outcome derivation without evidence-reference checks, read-only controller observation, and installer/runtime shutdown that preserves accepted storage. |
| `sdo/operator/sdk/`, `core/`, `runtime/`, `builder/`, `contracts/` | Production Go detector/program SDK with canonical object-ref parsing, deterministic controller and ordered response stages, typed plan execution and rollback, verified repair evidence, and generated contracts. |
| `sdo/operator/runtime/{plan_script.go,script_job.go}`, `operatorcli/script_{runner,capture}.go` | Production app-local script Job execution and private capture, reusing the existing Kubernetes proxy; no benchmark dependencies. |
| `sdo/operator/shellpath/`, `runtime/code_path*.go`, `runtime/cmd/sdo-shell-controller/` | Production shell execution and programming-model composition; no benchmark imports. The fixed image-bound controller starts without compiling accepted scripts. |
| `sdo/operator/runtime/replay/`, `builder/replay.go`, `cmd/sdo-replay/` | Transport-neutral offline tooling; real production plan semantics against isolated client-go fakes, without cluster credentials or benchmark imports. |
| `sdo/operator/apihistory/`, `kubeproxy/`, `operatorcli/evidence.go` | Production runtime observation storage, Kubernetes forwarding edge and offline read-only queries; no benchmark dependencies, replay or coverage authority. |
| `sdo/skills/` | Production authoring guidance installed in worker homes; generated SDK references and validator rule citations. |
| `libs/agent_cli/` | Production-neutral agent turn runner. |
| `apps/` | Deployment and evaluation inputs. `apps/toy/` owns the reusable tiny Kubernetes app, seed and cluster-state fault injectors/oracles; it has no SREGym dependency. |
| `benchmarks/sregym/{adapter,fastloop,faults,protocol,runner,experiments,analysis}/` | First-party benchmark integration, including launcher-owned helper lifetime and registry-backed abort through `adapter.processes`; `faults/description.py` owns shared discovery and capability validation, with catalogs recorded beside fastloop results and infrastructure classification before scoring; `protocol/native_receipt_go` owns typed receipt decisions and `protocol/submission_go` owns phase and acknowledgment decisions from generated contracts; Python transports/persists the results; scripted restore reads controller-rendered source through `sdo source`. Baseline recovery uses server-side apply as `sdo-deployer` and waits for application rollouts before the benchmark residue check; fastloop rows retain responder usage and mitigation timing from verified stage evidence if learning fails, and expose closure phase intervals, resolution path and playbook coverage from the native ledger. |
| `third_party/sregym/` | External benchmark harness Git submodule. |
| Historical bounded operator, shell health checks, trajectory recorder, UI, prompt stack, standalone fault injection | Excluded from production scope. |

This map does not claim model quality, deployment success across all applications, incident-repair success, or experimental improvement. Those require recorded live runs with exact configuration and evidence.

Program trust fingerprints executable sources and imported helpers, excluding Markdown and Go test files. Successful plan credit remains bound to the applied resources and requires an independently successful incident outcome; a global health failure with no explicit unrelated finding remains a failure. More than one binding of a program can earn equivalence evidence, but an incident contributes at most one trust success per program.

API history completeness is bound to a controller-written attempt generation
outside the recorder directory (`api-history.generation`). The memory edge
rotates it before launching an incident Job; launch fails if that write fails.
Sidecars rotate it again before opening the recorder. Agents mount both history
and generation read-only, and offline readers reject a stale or missing token
even after a failed recorder exits. This runtime boundary remains outside `.sdo/`.

Reflection work is grouped only by confirmed_root_causes resource sets. Go settles exact covered bindings and records observed fixed-capture false matches as required NoMatch cases. Unwritten bindings, unproven final plan equivalence and unbound writes remain typed questions in the resumed incident session; observations outside confirmed groups stay explicitly unassigned. No inferred cause, merge/split decision or trust credit is created for a question. Affirmative PlanApplication.final_equivalence_verified and an explicit fixed NoMatch are required for covered eligibility. Missing final proof or an absent fixed verdict remains a question. OutcomeRecord.reflection_eligibility version 1 records program revision, bindings and disposition before the outcome commit; trust and corpus publication consume that same result. Historical outcomes without trusted eligibility add no partial success credit. Newly authored programs may earn one origin credit through executable-v1 broker validation and a controller-verified bound fixture. Final completion object reads are cached once per distinct resource across applications. The final-equivalence field distinguishes an uncredited question from a failed execution without changing the historical applied/verification evidence. The fixed-negative provenance phase is SDK-defined, can label only NoMatch cases, and selects actual fixed-phase primitive observations. All materialized files still pass the same independent validator and broker.

Go counts confirmed causes with no recorded successful repair before the outcome commit and persists the count in settled eligibility. The broker allows a `no_recorded_repair` decline only for Go-flagged causes. First-party incident records expose the count.

Controller maintenance normally resets the configuration baseline after a planned redeploy. Its explicit `preserve_baseline` observation option keeps the last healthy baseline while evaluation is paused. The benchmark injection-validity barrier uses that option so partial invalid writes cannot dispatch responders, while valid latent faults remain visible after resume. Go owns the maintenance state and baseline policy; no benchmark identities enter this interface.

Repository staleness does not exist as a closure concept: every change to the operational repository is made by SDO, so an incident's investigation, closure and acknowledgment survive any advance of the accepted head, source, objective, health policy or detector scope. The health guard only requires the running detector bundle to match the accepted health policy (otherwise the controller restarts to adopt it and resumes the same work). Closure is decided by the live recheck under the detectors in force; a replayed closure that the broker already committed returns its existing receipt. The broker replays a verified proposal onto a moved head and re-verifies that the replay carries exactly the verified source.

The complete native receipt may reach the broker ledger before the controller persists its receipt state. After health adoption, the same ownership proof may resume that frozen closure’s idempotent broker effect, returning the existing receipt without publishing again. An absent/incomplete native receipt still fails closed. The restart fixture must cover both sides of the controller receipt persistence boundary.

A complete authenticated native receipt is recovered as an existing decision. Its replay does not re-run the independent closure decision after adoption. The durable effect identity still applies; incomplete receipts use the ordinary live recheck, which runs under the detectors now in force (a changed observation scope never supersedes a closure).
Automatic plans use the controller-derived incident namespace, with no hand-maintained application policy, content predicate, credential-delta gate or accepted-value restoration requirement. Save-time compile/vet/test/replay, post-apply verification, rollback, broker ownership and the state budget remain. Plans do not acquire cross-namespace or cluster mutation authority. See [automatic repair checks](plan-safety-authority.md) for the exact scope, ranking, fallback, defaults and RBAC changes.

The production `run.go` `LoopFactory` enables the mitigation menu by default. `SDO_RESPONDER_MENU=0` opts out; empty or `1` enables it and other values reject launch. The dispatcher advertises `SDO_MITIGATION_SESSION=1` only for enabled menus. Durable provider checkpoints use the independent `SDO_RESPONDER_SESSION=1` flag even when the menu is disabled. Menu execution uses the ordinary executor, accepted source, health verifier and controller credentials. No policy document or menu-specific execution gate is required.

Program statistics retain independently verified success credit across broker-validated program revisions and record revision-attributed failures. The incident-open coverage view lists all matching programs in stable program/revision/binding order, each explained finding once, and every unexplained finding once. Failed verification leaves landed repairs in place for the responder to assess. Response loops default to 10, verification to 5 minutes, and validation correction retries to 5.

### RL1 executable learning

Reflection uses only controller verification and responder session observations, never a benchmark grader or oracle. Each verified full or partial cause requires a diagnose/mitigate playbook, handling README, and a detector that references it; detector/playbook links are many-to-many. Go rejects incomplete coverage with retry diagnostics. Typed SDK gaps name exactly the affected cause resources. Session-authored replay fixtures do not require controller open/fixed capture provenance. The learning checks are compilation and an executed mitigation that changes diagnosis from Match to NoMatch. Accepted corpus bytes remain immutable and broker artifact ownership is unchanged. A new broker-validated program earns its initial trust credit from its controller-verified origin and replayed bound fixture, so threshold one can repair its first recurrence without a responder model call.

The installer’s immutable pending snapshot also defines pre-existing environment inventory. The controller reloads the captured object digests across failed health onboarding and restart. Only the same UID, configuration and ownership remain exempt from presence drift; replacements or changed objects are visible. Environment observations never populate healthy-onboarding authority, and legacy name-only records grant no environment exemption. Completion and reachability exclusions still require the original independent health checks. No environment object is labelled, deleted, or applied by this inventory rule.

A deliberate from-scratch reinstall can retire namespace-lifetime onboarding
with `sdo-operator reset-control-state --control-namespace <namespace>`, after
stopping the runtime. The onboarding package checks SDO ownership for the five
onboarding ConfigMaps, controller checkpoint and maintenance record before deleting any, uses
UID/resource-version preconditions,
and logs deleted identities. This command does not access application objects,
Secrets or repository memory. Warm reinstall/restart retains the immutable
record. Legacy final records without reachability evidence emit a typed
`reference_absent` note and authorize no exclusions.

The legacy controller checkpoint is recognized by its exact name,
`sdo.dev/state-revision` annotation and `runtime-state.json` key. Retiring it
prevents restored controller state from resurrecting old onboarding authority. Fresh installs discard old maintenance generations and use the existing ACTIVE default when no maintenance record exists. Legacy maintenance records require the exact valid closed data shape; newly written records carry SDO ownership labels.

The native installer and controller accept `--responder-standby=false` to force cold responder Jobs. The optional typed controller launch field `responder_standby` carries that choice. The idle standby is labelled `sdo.dev/standby` and is never counted as work. Incident liveness and receipt export resolve durable execution identities, including bound standby Jobs. Initial startup clocks remain immutable; `startup/attempts/<job>/startup-controller.json` and `startup-workers/<job>/startup-worker.json` retain typed attempt evidence.

### Composable code paths

`runtime.CodePathConfig` selects a named set of implementations, with optional
per-seam overrides. The zero value selects `go`. `CodePathRegistry` resolves
and validates detector source, coverage provider, response-stage names, memory
validation, activation after publication, and agent guidance before use. Unknown
names, duplicate stages and deterministic response without coverage return
`CodePathConfigError` naming the invalid seam. PIPE owns response-stage execution;
the registry selects stages and does not implement another stage runner.

The Go plugins retain compiled SDK detectors, diagnose-based coverage, isolated
memory validation, controller rollout and the existing role guidance. The
registry is transport-neutral and belongs to the production runtime scope. All
paths retain broker commit authority and the artifact ownership matrix above.


The shell path intentionally drops the requirement that detectors are deterministic
Go. The Go path remains the production default and retains its validation and
rollout behavior. Select shell with `--code-path=shell` on host, operate or
controller (also `install-existing` and `memory adopt-existing`), or with the typed launch value `code_path: {name: shell}`. Optional
`overrides` choose named implementations per seam. `--response-stages` writes
`overrides.response_stages`; response-stage execution belongs to PIPE.

| Seam | Go default | Shell |
|---|---|---|
| Detector source | Compiled SDK detectors | Accepted executable shell scripts |
| Coverage | Diagnose programs joined to findings | Empty |
| Response stages | `deterministic,agent` | `agent` |
| Memory validation | Shared structural/ownership and isolated compile/vet/test/replay | Text, size, credential and path hygiene plus the same actor/phase ownership grants |
| Activation | Controller build and rollout | Runner pulls accepted Git bytes; activation time is recorded |
| Role guidance | Existing responder/reflection/judge guidance | Short free-form notes and script guidance |

Plugins bind their mechanism dependencies at construction. Detector sources take
`DetectorSourceRequest` (repository root, application namespace, compiled
candidates); validation and activation calls receive only the operation inputs.
New implementations register by name without changing another plugin's call
signature. Unknown selections and incompatible stage/coverage combinations fail
with `CodePathConfigError`.

The installer provisions a long-lived `sdo-shell-detectors` Pod and read-only
results Service in the control namespace. Its dedicated application Role grants
only get/list/watch on explicit non-secret resources. The root filesystem and
accepted repository mount are read-only; `/tmp` is writable. The tool image
contains Bash, kubectl, curl, jq and Git. Initial health verification uses that
same runner against accepted scripts before starting the fixed shell controller.

The runner watches application resources with a trailing two-second debounce and
sweeps every 30 seconds. Each script has a 20-second process-group timeout, a
combined 16 KiB stdout/stderr tail, and at most four processes run concurrently.
Main script bytes are frozen from the accepted archive at discovery, so scratch
filesystem changes cannot replace later invocations. Two consecutive nonzero results fire; clear results and execution errors reset
the streak. Timeout/crash errors are retained as detector evidence and cannot fire
or establish health. A stable adapter emits one typed finding per fired script,
including the script path, output tails and accepted-commit attribution. Existing
controller dispatch, independent verification, outcomes and reflection stay shared.
Recovery requires all scripts to be clear over the 15-second stability window.

Shell memory uses the existing five artifact classes and the existing broker.
Only health scripts may be authored by the judge; responders draft incident
scripts and playbook notes/scripts in proposal checkouts and publish in reflection.
Shell catalogs read exact accepted Git blobs and carry file paths and text to the
agent. Their typed `code_path` tag permits those shell paths at task encoding;
the Go path retains its logical-memory boundary. Hygiene rejects a catalog over
the existing 512 KiB worker snapshot budget before publication. `outcome.code_path` records explicit selection/overrides. The runner report
carries `accepted_commit`, `activated_at`, `at`, per-script results and health;
the native ledger records `closure_phase_timestamps.shell_runner_activated` after
accepted reflection activation, without a Go controller rollout receipt.
