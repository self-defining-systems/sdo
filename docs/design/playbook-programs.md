# Playbook programs: reusable diagnosis and mitigation in the Go SDK

Status: agreed design, 2026-10-20.

## Problem

SDO is meant to get faster and cheaper as it learns: repeatable steps should
become programs, and agent reasoning should be reserved for judgement. The
luna-mixed-pilot run (Codex gpt-6-luna, 20-incident mixed-fault
hotel_reservation stream) showed no such trend.

- SDO resolved incidents about 2x faster than memoryless Codex from the first
  incident onward, but time to mitigation (35-60 s) and responder cost
  (230k-340k tokens) stayed flat as memory grew from 1 to 7 playbooks.
- Every learned playbook was prose with no scripts, and was incident-literal
  (`reservation-target-port-9999`).

Causes:

1. Reflection runs in a fresh session by default (since #5), and the native
   worker does not pass the responder's commands, so reflection cannot see
   what worked.
2. Reusable steps have no checkable form, so nothing makes them generalize and
   nothing can execute them without an LLM.

## Goals

- Repeatable diagnosis and mitigation become typed Go programs that are vetted
  offline. Natural language is reserved for judgement.
- Over time responders rely more and more on programs. Recurring incidents
  eventually resolve with no LLM tokens.
- The detector-to-playbook mapping and its bindings become type-checked,
  regression-tested and mostly derived.
- The SDK grows from evidence gathered in real incidents.

Non-goal: a persistent bash escape hatch. Bash is allowed during an incident
for exploration and quick mitigation, but it is never saved to `.sdo/`.

## Interfaces (`sdo/operator/sdk/program`)

```go
// Detectors: the existing sdk.Detector. Its job narrows to locating symptoms precisely.
// Finding.PrimaryResource and Finding.RelatedResources drive binding;
// Finding.Playbooks and Finding.ParameterBindings become optional hints.

type ProgramSpec struct {
    ID     string      // == fault class, role-based, e.g. "service-targetport-mismatch"
    Params []ParamSpec
}
type ParamSpec struct {
    Name     string
    Kind     sdk.Kind  // Service, Deployment, Pod, ConfigMap, ...
    Required bool
}
type Bindings map[string]sdk.ObjectRef

type Diagnoser interface {
    Spec() ProgramSpec
    Diagnose(ctx context.Context, v ReadView, b Bindings) (Verdict, error)
}
type Mitigator interface {
    Spec() ProgramSpec
    Mitigate(ctx context.Context, v ReadView, b Bindings) (Plan, error)
}

type ReadView interface {             // no mutating methods
    sdk.DetectionContext
    Logs(pod sdk.ObjectRef, container string, since time.Duration) (string, error)
    ProbeTCP(to sdk.ObjectRef, port int32) (ProbeResult, error)      // via SDO's probe pod
    ProbeHTTP(to sdk.ObjectRef, req HTTPProbe) (ProbeResult, error)
    DiffAgainstSource(ref sdk.ObjectRef) (Diff, error)
    DiffAgainstBaseline(ref sdk.ObjectRef) (Diff, error)
}

type Verdict struct {
    Result   Result      // Match | NoMatch | Unknown | NeedsJudgement
    Evidence []Evidence  // Resource, Fact, Observed, Expected
    Question string      // NeedsJudgement only; answered by the responder
}

type Plan struct {
    Actions   []Action   // closed set
    Rationale string
}
// Actions: ReconcileFromSource{Ref}, Rollback{Ref, ToRevision},
//          SetField{Ref, Path, Value, Reason}, RestartRollout{Ref, Reason}
```

- Programs never call an LLM. A judgement call is returned as `NeedsJudgement`.
- Programs cannot mutate anything. `Mitigate` returns a plan as data, and one
  audited executor applies it through the repair policy. With
  `repair_policy = commit`, that means a source commit plus a redeploy.
- `SetField.Path` is limited to an allowlist (ports, probes, image, selector,
  replicas, env, resources). The list grows from SDK gap evidence.
- Types that cross process boundaries (`Verdict`, `Plan`, program results in
  outcomes) get protobuf messages in `sdo/contracts/proto`, the canonical
  contract.

## Repository layout

```
.sdo/playbooks/<fault-class>/
    README.md        # front matter (fault_class, owner) + judgement-only guidance
    diagnose.go      # exactly one Diagnoser
    mitigate.go      # exactly one Mitigator (optional for judgement-only playbooks)
    *_test.go        # generalization tests over corpus cases
.sdo/lib/...         # freely organized shared Go packages used by playbooks
.sdo/corpus/<case>/  # shared: snapshots (incident open, after fix, healthy) + recorded primitive results + root-cause labels
```

Typed playbook proposals may provide either the README body or a complete
README with YAML front matter. The memory edge checks any supplied front matter
against the typed owner, fault class, and originating incident, then writes one
canonical front matter block. The body must be nonempty.

Because effects are confined to `Plan` values, shared libraries need no
read-only versus mutating split.

## Validation (broker and `draft-test`)

A playbook program is accepted only if:

- It compiles against the SDK and respects the import allowlist (no `os/exec`,
  `net`, `os` writes, `unsafe`). Libraries follow the same rules.
- Its bindings type-check: every required parameter is bound, kinds match, and
  no parameter is unused.
- It passes rename-invariance replay: resource names, label and selector
  values, container and port names, image repositories and tags, and ports are
  consistently remapped across snapshots, recordings and bindings. `Diagnose`
  still returns `Match` for renamed incident open, `NoMatch` after the renamed
  fix, and `NoMatch` for a renamed healthy peer.
- It passes the generalization tests on corpus fixtures. `Diagnose` returns
  `Match` at incident open, `NoMatch` after the fix, and `NoMatch` for a
  healthy peer of the same kind. `Mitigate` returns a non-empty plan on the
  broken fixture and an empty plan on the fixed one.
- The README contains no shell commands.

What is not a gate (2026-10-27): a verified cause without a program or detector
link, a skipped healthy replay (no controller capture), and a recorded fault no
incident detector reaches (`program.repeat-unreachable`) are recorded as
coverage gaps (ledger `reflection_coverage_gaps`, commit trailers, check
report), never refused: the repeat's act-verify loop is the safety net. The
memory edge derives the playbook `fault_class` from its ID, accepts literal
resource names and writes the index entry. When no finding selects a playbook
its mitigation is still replayed with the case bindings and must repair. A
program or detector defect on the repeat path is still blocking under its own
rule: `program.diagnose-no-match`, `program.mitigator-missing`,
`detector.invalid-finding`, `detector.fires-on-healthy`,
`program.repeat-unavailable`.

Rename replay keeps namespace names, IP addresses, image digests, binary
ConfigMap data, and HTTP probe paths unchanged. The corpus has no safe
substitute for those opaque or externally resolved values. Existing library
`metadata.yaml` files are tolerated during validation but are no longer
written or used as outcome provenance.

Corpus mapping replay is a broker gate for any change to detectors, programs
or the SDK. Every detector runs on every corpus snapshot, and every candidate
program runs with the bindings it would get. The replay reports precision,
recall and bad bindings against confirmed root causes. A change may not reduce
them.

## Candidate selection (from findings to programs)

- Candidates are every program whose `Params` kinds fit the finding's primary
  and related resources, plus any explicit hints. Bindings are filled by
  matching kinds.
- All candidates are diagnosed in parallel under one deadline (default 10 s).
  Anything still running at the deadline counts as `Unknown`.
- Hints are kept for now. The corpus replay measures whether they add anything
  over type-based selection. If they do not, they are removed and the mapping
  is fully derived.
- There are no cost tiers. Runtime and timeout metrics will show if structure
  is ever needed.

## Responder loop

The loop runs inside one responder session. SDO is a set of tools, and the
same session reflects at the end.

1. When an incident opens, SDO runs `Diagnose` on all candidates. The responder
   starts with the findings, ranked verdicts and evidence, any open questions,
   the matching playbook READMEs and the architecture summary.
2. Each iteration:
   - The responder chooses, per open finding:
     - `sdo plan <playbook> --bind …`, which shows a dry-run plan (choosing
       counts as approval);
     - more probing with kubectl or `sdo diagnose`;
     - or a session-only bash fix.
   - `sdo apply <plans…>` applies the batch through the repair policy, with one
     redeploy.
   - SDO verifies and returns the result to the session: each applied
     playbook's `Diagnose` again (expecting `NoMatch`), all detectors again,
     and `Diagnose` again on candidates for remaining or new findings. Faults
     hidden behind the fixed one surface here.
3. The loop stops when every detector clears, or after the loop budget: 5
   iterations by default, set with `--max-response-loops` or controller
   config. The incident is then reported as unresolved.
4. Ad-hoc fixes go through the same verification (`sdo verify`).

Tools: `sdo status`, `sdo diagnose`, `sdo plan`, `sdo apply`, `sdo verify`,
`sdo rollback`. Every call is logged to the outcome.

Failure signals recorded for reflection, the corpus and trust:

| Observation | Meaning |
|---|---|
| Plan applied, its own `Diagnose` still `Match` | the program is wrong: mark it suspect and offer rollback |
| `NoMatch`, but detectors still fire | another fault or an incomplete repair: the loop continues |
| A new finding after `apply` | a hidden fault or a regression |
| Fixed ad hoc, and some program would have matched | a binding or ranking bug |
| Fixed ad hoc, and no program applies | a coverage gap: compile a new program or record an `sdk_gap` |

## Agent-selected recurrence and coverage

Go diagnoses accepted, non-retired programs at incident open. `PlaybookCoverage` lists all Match results by program/revision/bindings, their explained finding fingerprints, informational verified counters and the menu option selecting their mitigation. Remaining active findings are listed explicitly. The responder chooses program mitigations and fixes uncovered findings; Go independently verifies closure. Completed repairs are undone only on explicit responder rollback. Partial-write compensation within an apply remains.

Coverage is copied unchanged into the outcome; application and action receipts show which candidate repairs actually ran. See [agent-selected repair evidence](../../.agents/skills/analyze-experiment/references/automatic-repair.md).

## Reflection

- `reflection_session = "resume"` becomes the default for this design, so
  reflection runs in the responder's own session.
- During the incident the responder writes multi-step or re-runnable checks as
  parameterized scratch scripts. They are never saved, but they are evidence
  for reflection.
- Reflection compiles what worked into the playbook's `Diagnose` and
  `Mitigate` (and shared `.sdo/lib` code), records corpus cases, and extends an
  existing fault class rather than adding a near duplicate. A proven check the
  SDK cannot express is returned as `learning_decision: sdk_gap`, with the
  scratch script and the missing capability. Gap records feed SDK growth.
- Reflection token usage is exported, so its cost is measured.

## Measurement

Three arms on the same stream:

1. today's design (fresh reflection, prose playbooks);
2. resume plus scratch scripts;
3. full playbook programs.

Per incident: requests, tokens and time to mitigation (judge time excluded),
`Diagnose` runs and verdicts, loop iterations, reuse on new services, SDK gaps,
verified counters and coverage of incident findings.

## Memory maintenance

Maintenance happens at two levels, and the corpus replay gates both.

**Local, in per-incident reflection (same session):**

- Tighten a program that just misfired. A misfire is a `Match` whose plan did
  not resolve the incident, or whose confirmed root cause differed.
- Extend an existing fault class rather than adding a near duplicate.
- Fix the binding or ranking that forced an ad-hoc resolution.

**Global, in a maintainer pass:** a reflection-like task triggered by
deterministic signals that the broker computes from the replay and outcomes
after each accepted change.

| Signal | Suggests |
|---|---|
| Two programs return `Match` on the same cases with equivalent bindings | merge |
| One program matches cases with different confirmed root causes, and its plans succeed on only some of them | split |
| A program matches healthy or wrong-root-cause cases above a threshold, or its plans keep failing | tighten; retire if tightening fails |
| A precise, snapshot-only program could have caught an undetected incident | promote to a detector |
| A detector fires only where some program matches | replace the detector with the promoted program |
| Several detectors or programs reimplement the same check | extract it to `.sdo/lib` |

The maintainer proposes typed edits: merge, split, tighten, retire, promote,
or extract. The broker accepts an edit only if replay precision and recall do
not drop and every previously solved case still has a matching program. A
retirement that strands a case needs a recorded justification. An edit resets
the trust count of every program it touches. Nothing is retired for disuse:
rarely needed programs are the long-tail knowledge memory exists to keep.

**Detectors and diagnosers:** these remain separate roles that share code.

- A detector is a scheduled, locating symptom check with persistence rules. A
  diagnoser is an on-demand root-cause check over bound resources.
- Shared `.sdo/lib` functions hold the logic once, and both kinds of code
  import them.
- Detectors have independent detection entry points. Diagnose runs only when
  handling a finding, with roles bound from that finding.
- Health detectors stay symptom-level, owned by the health judge, and
  independent.

## Deferred

- Whether the SDK should eventually expose a single interface for detectors
  and diagnosers. Decide from promotion statistics.
- Fixture size budgets in `.sdo/corpus`.

## Implementation phases

1. **SDK and validation:** the `sdk/program` types and finding bindings; record-and-replay `ReadView` and the corpus fixture format
   in `sdktest`; a module layout that lets detectors and playbook programs
   both import `.sdo/lib`; validator support (import allowlist, binding
   type-check, rename-invariance replay, generalization tests); protobuf
   messages for `Verdict` and `Plan`.
2. **Controller and loop:** candidate selection, parallel `Diagnose` under a
   deadline, the plan executor through the repair policy, verification,
   responder tools, the loop budget, and outcome logging plus trust statistics.
3. **Agents:** resume-mode reflection by default, the compile-to-programs or
   `sdk_gap` reflection contract, responder prompts for program verdicts and
   scratch scripts, and reflection token export.
4. **Evaluation:** the scripted coverage gate, then live recurrence evaluation.

Each phase updates `docs/architecture.md` (ownership: responders author
playbook programs, the broker validates them, the controller executes them)
and `.agents/skills/analyze-experiment/references/`.
