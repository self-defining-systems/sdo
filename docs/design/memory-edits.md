# Operational memory: direct edits and one acceptance check

Status: selected design for PR 5c, 2026-10-21. Agents author final files in
their proposal checkout; Go validates and commits them. This note specifies
the resulting contract, not an assertion that the unbuilt gates already run.
It implements the user's 2026-10-18 decision to remove artifact translation.

Evidence: [architecture](../architecture.md), memory PRs
[11](https://github.com/self-defining-systems/sdo/pull/11),
[13](https://github.com/self-defining-systems/sdo/pull/13),
[14](https://github.com/self-defining-systems/sdo/pull/14),
[15](https://github.com/self-defining-systems/sdo/pull/15),
[16](https://github.com/self-defining-systems/sdo/pull/16),
[18](https://github.com/self-defining-systems/sdo/pull/18), and
[19](https://github.com/self-defining-systems/sdo/pull/19).
Baseline checkout: `d8ba372f4`; playbook/drift evidence: `8ec987c83` on
`vic-lsh/feat/drift-p1`, including its `docs/design/playbook-programs.md`
and `docs/design/drift-and-invariants.md` (foreign NetworkPolicy admission).

| Part | Implementation status at these revisions |
|---|---|
| Layout, per-detector manifests, logical IDs, validator, CLI, skills | Built in the memory series; CLI defaults to structural/semantic checks, ownership needs a base, compile is separate |
| CLI/broker parity | Built for static validation only (`broker/parity_test.go`) |
| Typed diagnostic retry | Specified/implemented by #19, not in this checkout |
| Programs, generated generalization/rename tests, captured corpus references | Built on drift-p1; still use typed proposals/materialization |
| Full acceptance entry point, input bundle, direct-edit workers | Unbuilt; this design |
| Mapping precision/recall regression, applying mitigation to fixtures | Unbuilt; plan-shape tests and labeled-case replay are insufficient |
| Missing-peer witnesses, protected derived files, repository migration | Unbuilt; this design |

## Decision and requirements

Reflection edits `.sdo/` as the responder already edits application source.
Its result carries decisions and logical IDs, never artifact contents. The
broker derives the diff, independently repeats the check, and commits exactly
the checked tree. Outcomes remain controller-authored through the Go broker.

| Requirement | Enforcement |
|---|---|
| R1: every broker check is runnable by the agent | Dispatch provisions all immutable inputs and the identical validation runner locally; missing inputs fail closed, never disable a gate |
| R2: one command covers acceptance | `sdo-operator memory check` runs the complete acceptance pipeline with environment-supplied context; no actor/base/layer/toolchain flags are needed |
| R3: structural parity | CLI and broker call `memory/check.Check` with the same request contract, inputs and rule catalog; catalog coverage, fault injection and end-to-end verdict parity are CI gates |

Translation caused README/front-matter, casing, duplicated provenance and
cross-field failures before correction could help. Files have one parser and
one accepted representation. This removes `OperationalArtifactProposal`, not
typed task/result contracts or independent validation of agent-written code.

## Files and ownership

`memory/layout` (#11) owns classification, paths, parsers and writers. Extend
it for programs, libraries and corpus; callers do not invent another layout.
Corpus/lib support playbooks and diagnostics; they do not add independent owners.

| Path under `.sdo/` | Authorship |
|---|---|
| `playbooks/<id>/README.md` | Responder writes final front matter and guidance; judge may author only `health-objective`; Go owns provenance fields |
| `playbooks/<id>/{diagnose,mitigate}.go`, `*_test.go` | Responder; exactly one Diagnoser, optional Mitigator; judgment-only programs may omit mitigation |
| `playbooks/<id>/scripts/*.sh` | Existing prose playbooks retain scripts and current script checks; program playbooks forbid scripts and shell guidance, as on drift-p1 |
| `lib/<package>/*.go` | Responder shared deterministic helpers, covered transitively by import checks and replay |
| `diagnostics/detectors/<health\|incidents>/<id>/{manifest.yaml,*.go}` | Judge owns health; responder owns incident detectors; class and owner must agree with location |
| `diagnostics/traffic/{generators,workloads}/...` | Existing ownership table: judge, except responder incident-scoped traffic during reflection |
| `corpus/<case>/case.json` | Agent authors capture references, bindings, label associations and negative-witness selections; never raw observations |
| `corpus/<case>/{open,fixed,healthy,recordings,labels}.json` | Go-derived, protected replay files; keep the sdktest five-file read format, extending it for phase-specific recordings |
| `playbooks/README.md`, generated registries/module wiring | Go-generated indexes/build inputs; never co-edited by agents |
| `outcomes/*`, `schema-version`, `lifecycle-provenance.yaml` | Existing Go/controller/Upkeep writers; no reflection/judge edits |
| `goal.md`, `arch.md` | Existing human and deployer/Upkeep ownership |

Program ID is directory ID and `ProgramSpec.ID`; README `fault_class` is a
separate valid logical mechanism ID. Several programs may share a class.
Replace `layout.PlaybookFaultClass` equality and the builder's corresponding
equality rule; validate explicit associations instead. A full README has one
front-matter block, not a body field embedded inside another serialization.
Narrative and authored Go still use logical references; `.sdo/` in narrative
is `text.physical-path`, with a precise location and correction hint.

Go initializes owner/class and originating incident/commit for new artifacts
from the task and kind ownership policy. Refinements retain original provenance; the
current incident is an additional corpus/evidence association, not a rewrite.
Agents preserve existing protected fields and omit new provenance fields.
The check rejects conflicting new protected values and any change to accepted
ones. Go fills omissions or verifies exact Go-scaffolded values in scratch;
authored fields such as fault class are never silently fixed.

## Complete inputs in the sandbox (R1)

Go seals a versioned `MemoryCheckContext` outside the checkout and exposes
its path through `SDO_MEMORY_CHECK_CONTEXT`. `SDO_ACTOR`, `SDO_PHASE`,
`SDO_BASE_COMMIT`, `SDO_OPERATOR_ROOT` and `SDO_CAPTURED_CORPUS_DIR` are
convenience projections of that context, not independent authority. A
conflict is an error. The context binds task/proposal ID and contains:

- Exact accepted base/outcome commit and Git objects, actor, phase, bootstrap
  state, verified closure, confirmed causes, repair policy and eligibility.
- Complete accepted corpus and prior outcomes used by replay, their hashes,
  accepted mapping baseline, healthy detector baselines and source identities.
  Prompt history may be bounded; validation history is never truncated.
- Read-only controller captures: `open@detect`, `open@settle`,
  `open@pre-apply`, `fixed`, legacy `open` when present, and recordings per
  phase. A drift case names its original detect capture explicitly.
- Accepted rendered source, baseline manifests, rollback revisions and
  recorded action/effect evidence needed by plan replay; no live-cluster read
  or external verdict is needed for an acceptance decision.
- Validator/rule-catalog version, SDK and builder identity, immutable runner
  image, Go version, module graph/cache seed, deterministic seeds, deadlines
  and resource limits. Go/toolchain/modules and `bash -n` are installed.

The worker image includes the same hermetic runner as the validator Job.
`memory check` launches it in a local isolated child sandbox with read-only
candidate/inputs, writable scratch and private caches, no egress, publishing
credentials or cluster credentials. Provider access stays in the outer agent
process. The local runner checks isolation with the same canary as the Job.
Image CI exercises this from both supported providers' actual confinement.
No nested privileged Docker or permission escalation is required; an image
without the supported isolation helper fails dispatch preflight.

Context/capture hashes are anchored in Go-owned task state, with read-only
mounts in the worker. The broker reconstructs its request from that state,
not agent-controlled environment variables or a claimed successful report.
Missing captures/cache/toolchain/source evidence are explicit infrastructure
failures. Unavailable evidence is never converted into synthetic success.

## One pipeline and verdict (R2, R3)

New package `sdo/operator/memory/check` sits above `memory`, `layout`,
`validate`, builder and replay, avoiding the existing validate-to-memory
dependency cycle. Both callers use this sole acceptance function:

```go
Check(ctx, Request{Candidate, Context, Result}) (Report, error)
```

`Candidate` includes committed, staged, unstaged, deleted, untracked and mode
changes against the pinned base, using one shared tree builder. Freeze it
before executing tests. `Result` is the small final decision in the sandbox's
environment-supplied result slot, later returned unchanged by the worker.
The report binds raw tree, prepared tree, context, result and validator
digests, executed rule IDs, diagnostics and replay metrics. It proceeds:

1. Verify context/identity/eligibility and result/diff consistency. Enforce raw
   ownership against base, including attempts to edit Go-owned files/fields.
2. In scratch, Go resolves capture references, initializes protected metadata
   and regenerates indexes. Validate the complete prepared tree and ensure
   generated changes are limited to declared derived outputs. No authored
   bytes are reformatted, translated, repaired or dropped.
3. Run every static, compile, vet, detector, traffic and program gate below;
   dependency failures explain why later gates are blocked, never “passed.”
4. Run generalization, rename, mitigation and whole-corpus mapping replay;
   emit an acceptance verdict only when every required gate passes.

CLI and broker both call `Check`, which runs the shared pipeline in a local
sandbox or an isolated Job respectively. Transport cannot change rule sets or
inputs. `Check` includes receipt verification, bound to every report digest
and isolation attestation; no private caller-side acceptance predicate remains.
Optional fast static feedback cannot authorize acceptance. The broker runs
its own Job; an agent's self-check receipt never authorizes commit. Publication
copies only the prepared tree that was actually checked.

`memory check` always selects all acceptance gates. Partial layer selection is
removed from this command; internal layered validation remains for tests.
Exit 0 prints `PASS` with context/tree identities, 1 prints `FAIL` and sorted
`path:line: rule: message (hint)` diagnostics, 2 reports infrastructure/usage
errors (also with stable IDs). `--json` changes presentation only, using an
extended `MemoryCheckReport` and the existing `MemoryDiagnostic` conversion.
Diagnostics map generated positions back to authored files and include
program/case/binding/phase, expected/actual verdict and
metric numerator/denominator where relevant; complete logs remain readable.

Atomic publication still compares accepted HEAD to the pinned base. A race
invalidates that context (`context.stale`); Go supplies a fresh checkout and
bundle for another check. Locks, idempotent receipt lookup and atomic commit
are transaction operations, not extra content rules. Bootstrap-closed,
accepted-outcome-required, verified-source and decision/ID consistency checks
move into the common context/envelope gates; no private content gate remains.

### Gate inventory and destinations

| Existing location/check | Shared acceptance destination |
|---|---|
| `validate/{load,semantic,ownership}.go`, broker static gates | Retain rules/parsers in `validate`; `check.Check` invokes all, with base and authoritative actor/phase |
| `validate/compile.go`, `operatorcli/validate.go`, builder detector/traffic workspace | Same runner builds, vets and tests all authored packages; preserve import/safety, manifest/Spec, traffic scenarios/workloads and healthy-baseline checks |
| `memory/artifact_proposal.go`: README parsing, nonempty body, IDs, names/size, role/deletion/protected detector checks | Move any missing rule to `validate`, reusing layout parsers; conflicts between typed and Markdown fields disappear with the duplicate representation |
| Builder `programs.go`: source layout, constructors/interfaces, imports, library metadata, README no-shell and corpus labels | Static rules in `validate/program.go` and `validate/corpus.go`; builder only stages/builds/registers checked files |
| Generated `sdktest` binding/generalization/rename/mitigator tests | Mandatory harness emits `program.binding`, `program.generalization`, `program.rename`, `program.plan-shape`; agent tests cannot replace it; algorithms stay in `sdk/sdktest` |
| `ResolveCorpusReference`, `ValidateCapturedCorpusTree`, `ValidateCorpusProvenance` | Shared capture resolution plus `corpus.reference`, `corpus.provenance`, `corpus.recording`, `corpus.binding` rules; remove native-broker-only session comparison |
| Broker Job receipt/schema/cache checks | Shared context and receipt verification; same toolchain, source identities, limits and canaries locally and in Job |
| Promised mapping and applied-repair guarantees | Implement `memory/replay` gates `corpus.precision`, `corpus.recall`, `corpus.coverage`, `program.repair`; not covered by today's generated tests |

Every rule has one descriptor in a common catalog with its stage, dependencies
and applicability. A catalog test enumerates rule emissions and requires a
failing witness for each rule through CLI and broker; injecting a sentinel
rule into any stage must reject both. Integration parity asserts identical
executed/applicable rule sets, prepared-tree digests, full diagnostics and
verdicts. Removing a layer, swallowing a diagnostic or adding a caller-only
rule fails CI. Applicability is determined by context/tree, never caller.

## Corpus references, generalization and repair

`case.json` references logical incident/capture IDs plus content digests and
selects program/confirmed-cause associations, bindings and negative witnesses.
Cause references bind an immutable outcome digest and confirmed-cause index.
For example (digests abbreviated here only):
```json
{"capture":{"incident_id":"inc-42","open_at":"detect","open_sha256":"…","fixed_sha256":"…","recordings_sha256":"…"},"program_id":"deny-policy","confirmed_cause_id":"cause-1","bindings":{"policy":{"kind":"NetworkPolicy","namespace":"app","name":"foreign"}},"negative":{"transform":"networkpolicy-allow-all/v1"}}
```
Go resolves it through an immutable capture manifest; arbitrary paths, inline
snapshot bytes, made-up observations, mismatched hashes and unknown evidence
are rejected. Go alone copies captures and writes provenance/labels/indexes.
Agents can inspect every input and edit case associations. Accepted evidence
and positive cases cannot be deleted/relabelled to improve a replay score.
Synthetic negative cases require a declared deterministic transform/reason;
they cannot supply positive evidence or increment success/trust counts.
Use type-specific budgets (retain the existing 8 MiB case limit initially),
not the 256 KiB authored-source limit or worker JSON transport limit.

Generated replay requires Match at verified open, NoMatch after the fix and
negative binding coverage. Validate required parameter presence, kinds and
object availability separately; reject undeclared bindings and roles unused across the suite. Optional parameters are valid and
tested both absent and present where captures support them. Do not require
equal binding-map lengths. Recordings are phase-specific; an unrecorded
Logs/probe call fails with its method/arguments, never returns an empty value.
Rename resources, labels/selectors, container/port names, ports and image
repositories/tags consistently across snapshots, bindings, negatives and
recordings, preserving the documented opaque-value exclusions in sdktest.

### Foreign objects and a missing healthy peer

Foreign NetworkPolicies may have been removed by the verified repair, leaving
no same-kind peer. Negative coverage is over valid bindings, not enumeration
of surviving objects. The one command selects and records one of:

- A captured healthy same-kind peer, with required related roles preserved.
- A Go-owned, versioned same-kind benign transform in the fixed snapshot,
  plus replay of the original binding with the object absent after deletion.

For NetworkPolicy, construct a valid policy from the captured object's
identity/selector in the fixed snapshot with explicit Ingress and Egress
allow-all rules. This tests the same bound relationship with a nonrestrictive
policy, rather than choosing an irrelevant unselected object. Validate its
API shape and the SDK transform's semantic preconditions independently of the
program. The negative must return NoMatch, including after rename; a program
that matches every source-absent policy fails. The unchanged fixed snapshot
must also return explicit NoMatch for the retained deleted-object binding.
An empty binding map, Unknown, or “no peer found” never satisfies either test.
Witness provenance binds transform version and input/output hashes; it never
claims the derived peer was observed or changes the captured fixed-state hash.

Go derives source/baseline diffs for these fixtures from pinned manifests;
it never invents probe/log results. Required unsupported observations fail
`corpus.recording`. Other kinds need an equally checkable benign transform
or an observed peer; otherwise `corpus.negative-witness-missing` rejects the
update and the agent reports an SDK gap. No all-clear-health assumption turns
an arbitrary fabricated object into a healthy witness. This retains the
same-kind near-miss guarantee and adds deletion-boundary coverage; it proves
fixture generalization, not universal correctness or live connectivity.

For each Mitigator, require a nonempty valid plan at open, an empty plan at
fixed, then apply the actual open plan to a copy of the broken fixture using
production action/patch semantics. Diagnose the resulting state: require
NoMatch and no new fixture faults, also after rename. Never substitute the
known fixed snapshot as the result of application. Extract pure plan semantics
from `runtime/plan_executor.go` into transport-neutral `operator/plan` for
both runtime and replay. Source reconciliation/rollback use pinned manifests;
restart/status/probe effects need action-bound captured transitions. If an
effect cannot be replayed, `program.repair-replay-unavailable` fails closed;
diagnosis-only learning remains possible, without claiming a working repair.
Source absence grants no deletion action; the current SDK's closed action set
still applies. A foreign-policy invariant may therefore be diagnosis-only.

Mapping replay runs base/candidate detectors, selection/binding and all programs
on the same complete frozen corpus. Extract selection/binding into
`sdk/program/candidates.go`, shared with runtime, avoiding a runtime import cycle.
Use confirmed cause IDs/outcomes as truth, not agent-assigned names or hints alone.
Compare exact precision TP/(TP+FP) and recall TP/(TP+FN), globally and per
confirmed mechanism; reject drops, bad required bindings and any previously
solved case stranded by deletion/retirement. A zero denominator is explicitly
N/A and cannot hide an uncovered positive. Compare both trees on new cases too;
require candidate coverage of each newly claimed positive. Changes to shared
libraries, detectors, programs, associations or SDK identity trigger replay.
The baseline scores are recomputed, not accepted from the agent. #5c provides
no retirement override that weakens these gates.

## Agent loop, deletion and migration

Reflection reads skills and captured evidence, edits allowed files and writes
its small result to the supplied result slot, runs `memory check`, and fixes
diagnostics until green within its turn budget. It returns `updated` with
logical IDs, or `no_change`/`sdk_gap` with concrete reasons and no authored
diff; SDK gaps may also accompany an update for other causes. Go checks the
result against the actual diff and evidence. A timeout is failure, not green.
#19's durable `MemoryDiagnostic` retry remains the fallback: two retries,
three dispatches total, recorded before checkout release; infrastructure or
stale-context failures are not misclassified as learning corrections.
Judge tasks use the same loop and full check with judge/initial ownership,
objective/source evidence and health baselines; health acceptance stays
independent. Bootstrap generic health/traffic installation becomes an explicit
Go-derived step also exercised by the command, not hidden materializer work.

Delete active `OperationalArtifactProposal`/Proposed* producers and consumers,
`MaterializeArtifacts`, `FillResponderProposalContext`, Python artifact draft
views, `program propose` JSON, proposal-validation scratch projection and
draft-specific correction loops. Retain scaffolding as direct file creation,
generated result contracts, SDK-gap decisions and #19 diagnostic correction.
For wire compatibility first deprecate old proto fields/messages and reject
their use on the direct-edit capability; remove/reserve them only at a
versioned contract transition with regenerated bindings and buf checks.

Upkeep first runs existing v1/v2-to-v3 layout/ID migration, then a versioned
program/corpus migration: retain authored bytes and original provenance,
regenerate indexes, import archived Go captures and hash-bound references.
Never invent provenance for old inline corpus. Mark unverifiable legacy data
explicitly: it stays readable and in regression replay, but cannot support
new learning/trust without recapture. Affected updates fail a shared evidence
diagnostic. Dispatch preflight must provision all required archived inputs;
pruning sessions cannot remove the sole copy of accepted replay evidence.
Pin worker/validator capability and schema together; drain old tasks before
switching and retain old accepted commits for rollback.

## Alternatives

Scores: 1 poor, 5 strong, equal weight; design estimates. “Trusted surface”
means small machinery beyond the unavoidable re-check of all agent content.

| Alternative | Structural parity | Low cost/new artifact | Small trusted surface | Sandbox simplicity/cost | Migration | Total |
|---|---:|---:|---:|---:|---:|---:|
| A: direct files + one complete shared check (chosen) | 5 | 5 | 5 | 3 | 4 | 22 |
| B: typed proposals, schema generated from validator | 3 | 2 | 2 | 3 | 5 | 15 |
| C: edit files, submit patch blob for broker application | 4 | 4 | 3 | 3 | 3 | 17 |

A adds each kind once to layout/validation and removes content serializers.
B can align field syntax but still translates trees, ownership and cross-file
invariants; equal validation needs the same sandbox and projection anyway.
C can reach parity only by sharing A's checker after exact patch application;
it adds patch parsing, modes/deletion/base handling and transport limits
without reducing validation cost. All three must re-check README/code,
bindings, references and tests. None makes agent-written labels trustworthy.

## Ordered implementation and validation

Each step starts with a failing contract test and is independently testable;
estimates are focused implementation time, excluding CI queue/review.
Prefixes below are relative to `sdo/operator/` unless stated otherwise.

| Step | Files / deliverable | Estimate | Independent check |
|---|---|---:|---|
| 1 | `memory/check/{context,check}.go`, `sdo/contracts/proto/sdodev/contracts/v1alpha1/memory.proto`: input/report contracts and catalog | 75m | Missing/hash-mismatched inputs and catalog witnesses |
| 2 | `memory/layout/{paths,tree,playbook}.go`: program/lib/case kinds, ID vs class | 75m | Classification, full README and distinct-class fixtures |
| 3 | `memory/check/prepare.go`, `validate/ownership.go`: raw tree, protected fields, derived overlay/index | 90m | Modes/deletions/untracked, forged metadata, stable prepared digest |
| 4 | `memory/corpus_reference.go`, `validate/corpus.go`: reference resolution and capture provenance | 90m | Tampered/missing snapshots, recordings and prior incident references |
| 5 | `validate/program.go`, `builder/programs.go`: move static program/README/import gates | 75m | Every migrated rule emits an actionable diagnostic |
| 6 | `memory/replay/program.go`, `sdk/sdktest/{generalization,rename}.go`: mandatory harness, optional bindings | 90m | Renamed cases and omitted optional parameters |
| 7 | `sdk/sdktest/negative.go`, `memory/replay/negative.go`: deletion/benign-policy witnesses | 90m | No surviving peer succeeds; match-all-policy/Unknown fails |
| 8 | `plan/actions.go`, `runtime/plan_executor.go`: extract pure production patch semantics | 90m | Runtime/replay action conformance, source-absent refusal |
| 9 | `memory/replay/repair.go`: apply plan and diagnose resulting fixture | 90m | Plausible wrong plan fails; unavailable effects fail explicitly |
| 10 | `sdk/program/candidates.go`, `runtime/program_candidates.go`, `memory/replay/mapping.go`: shared selection/binding and corpus replay | 90m | Wrong hint, bad binding, all-positive/empty-program controls |
| 11 | `memory/replay/metrics.go`: precision/recall and preserved-case gates | 75m | Score dilution, relabel/deletion and per-class regressions |
| 12 | Worker/validator Dockerfiles, `worker_registry/kubernetes*.go`, `broker/kubernetes_validator.go`: seal/mount bundle and local runner | 90m | Image preflight and actual provider confinement smoke |
| 13 | `operatorcli/{memory,validate}.go`, `broker/{validation,validator_job}.go`: both call full Check; extend parity tests | 90m | Real no-flag command vs publish, every gate and receipt |
| 14 | `runtime/registered_reflection.go`, `sdo/agents/responder/reflection*.py`, `worker.proto`: direct edits/result slot | 90m | Fake worker edits, green update, no-change/gap and #19 retry |
| 15 | `lifecycle/coordinator.go`, `artifacts/bootstrap.go`, `sdo/agents/health_judge/worker.py`, `lifecycle.proto`: judge direct edits | 90m | Initial/refinement ownership, independent health evidence |
| 16 | `memory/layout/migrate.go`, `operatorcli/migrate_memory.go`: versioned repository migration | 90m | Legacy replay, absent archives, idempotence and rollback |
| 17 | Delete materializer/draft/propose paths and tests; regenerate contracts; update `sdo/skills/` | 90m | No active legacy references, buf compatibility, skill examples |
| 18 | `broker/parity_test.go`, runtime integration fixtures, `docs/architecture.md`, `.agents/skills/analyze-experiment/references/` | 90m | Scenario matrix, full Go/Python suites, format/error checks |

The scenario suite executes the real `sdo-operator memory check` subprocess
with only provisioned environment (repeat with `--json`) and real broker
publication/isolated runner against copies of the same Git tree and bundle.
It compares exit/verdict, exact diagnostics/rules, metrics and prepared hashes;
successful broker commits must contain exactly that tree. No mocked validator.

| Model-shaped scenario | Required identical outcome |
|---|---|
| Complete front-matter README | Pass without wrapping/translation; conflicting protected metadata or empty body fails |
| Program name differs from fault class | Pass when directory/Spec IDs agree and evidence mapping resolves; mismatched Spec fails |
| Optional binding omitted | Pass if required roles/kinds and negative coverage hold; missing required binding fails |
| Malformed, then fixed in the same checkout | Same named failure, then pass after file correction; no proposal JSON correction step |
| Refine detector from a prior incident | Pass preserving original provenance and prior cases; rewriting provenance fails |
| Physical `.sdo/` path in narrative | Both reject `text.physical-path`; logical reference correction passes |
| Long history with ancient regression | Bounded prompt, complete replay; old precision/recall/coverage regression fails |
| Foreign policy removed; no healthy same-kind peer | Pass diagnosis-only with benign derived policy and retained absent binding; match-all-policy, missing bindings, Unknown and unavailable source fail |
| Wrong repair, forged recording, missing cache, private gate | Respectively repair diagnostic, provenance diagnostic, infrastructure error, parity-test failure |

Run focused operator/SDK/runtime/worker tests per step, then repository
`scripts/format_code.sh` and `scripts/check_errors.sh`; cap fuzzing at 5m.
Document-only baseline verification: existing
`go test ./broker -run TestCLIAndBrokerAgreeOnStaticValidation -count=1`
passes; the expanded scenario suite above is an implementation deliverable.

## Open decisions and recommended answers

- **Negative witnesses:** ship the Go-owned NetworkPolicy transform with
  deletion coverage now; require explicit SDK support for other absent kinds.
- **Replay cost:** start with complete replay and fixed limits; cache only by
  all tree/context/SDK/corpus digests, never sample away a required case.
- **Unsupported mitigation effects:** accept diagnosis-only memory and report
  an SDK gap; grow effect replay from captured production action evidence.
- **Schema/wire rollout:** allocate the next schema version after the drift
  merge; version direct-edit capability explicitly and drain old workers.
- **Legacy corpus and retention:** preserve accepted evidence durably outside
  session TTL; require recapture where authenticity cannot be established.

## Judge migration implementation

The native health judge now edits final checkout files through
`health-judgment-direct-v1`. Its structured result carries metadata and detector
IDs. The former proposal writer and translation helpers are removed; deprecated
wire descriptors remain solely for Buf compatibility and their payloads are
rejected by direct-edit workers and Go lifecycle dispatch.

`validate.Check` explicitly calls `memory.PrepareHealth` after static validation
and validates the prepared tree again before compilation. This installs generic
service-endpoint and traffic detectors from final workload files without
overwriting conflicting authored contents. The broker includes the prepared
files in its accepted diff. Independent health verification still follows the
accepted judge commit. Source-only health-detector refinements are allowed; the
other ownership boundaries remain enforced. This completes the native judge
portion of steps 15 and 17; the other unbuilt gates above keep their stated scope.
