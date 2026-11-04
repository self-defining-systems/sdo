# Playbook generalization: localize, deduplicate, organize by repair method

Status: proposed (2026-11-04), revised after an adversarial review (see "Review changes"). Evidence: live SREGym rounds P4/P5 on social-network
(process_frozen, pod network impairment, Redis ACL revoked).

## Problem

Learned memory is organized per (symptom x service) and binds too widely:

1. Responder incident detectors filter a journey SLO finding (`scenario-slo.home-timeline-read`) and
   bind a hard-coded Deployment name. Hint bindings apply to every program with a `deployment` role,
   and the learned stall diagnoses ("matches source, not yet restarted") match any healthy Deployment.
   Result: 3-6 restarts across every service on the failing journey.
2. `RunScript` must be byte-identical to a receipted command, so a recorded
   `kubectl exec <live-pod-name>` was frozen into a program and later hit the wrong Redis.
3. Reflection publishes a second copy of the same repair for a second service, and folds a new repair
   method (Redis ACL restore) into each symptom-named playbook. Adding a required `redis-pod` role that
   no detector binds made the refined playbook stop matching.

There is no deterministic per-component signal a detector or diagnose can use to pick the faulty
component from journey-level findings.

## Mechanism

### (a) Localization: `sdk/localize` (Go SDK, pure)

A new SDK package computes suspects from observations the controller already gives every detector and
program (no new observation, no runtime or proto change):

- `localize.FromContext(ctx sdk.DetectionContext, workload string) Result` judges the health traffic
  window (`traffic.Judge`) and does path algebra over scenario paths (`Target.Service` + `DependsOn`):
  suspects = the components on every failing path (when the failing paths share none, on any failing
  path) plus every Service with sustained dial failures of its own, even one no scenario lists. A
  healthy scenario never removes a suspect, and dial failures never narrow the set: membership of a
  healthy path and dial failures only order suspects (see "Review changes"). Each suspect Service maps
  to its backing Deployment/StatefulSet by selector, with an evidence string.
- `Result.Implicates(ref) (bool, string)`: diagnose gate; a program returns NoMatch for a component the
  current findings do not implicate.
- `localize.BindSuspects(f sdk.Finding, r Result, role string) []sdk.Finding`: an incident detector
  emits one finding per suspect workload with `ParameterBindings[role]`, instead of a literal name.
  Each copy's fingerprint is the finding's fingerprint (or the controller default,
  `sdk.FindingFingerprint`) suffixed by the workload, so the copies stay distinct findings.

Examples (synthetic shapes of P5 rows): post-storage delay fails both timeline reads while the landing
page is healthy -> {post-storage-service, its cache, its store} first, then the front end both reads
share (reachability failures rank first);
home-timeline Redis ACL -> {home-timeline-service, home-timeline-redis} (plus any front end on the
failing path), even when a healthy write lists the Redis; the ACL program's own
evidence (an `Observe` of `ACL DRYRUN` on the bound Redis's Pod) then selects the Redis.

Ownership: SDK (production, deterministic, no I/O). Health detectors and the judge are unchanged.
Responders use it in the incident detectors and playbooks they own. Binding still flows through the
existing finding `ParameterBindings` (binding source `hint`/`finding`).

### (b) Publication checks in the shared `validate.Check` (static layer)

Run by both `sdo-operator memory check` and the broker, on changed paths only (existing memory keeps
validating until edited):

- `playbook.live-object-name`: a changed Go or script file under `.sdo/playbooks/`, `.sdo/lib/` or
  `.sdo/diagnostics/detectors/incidents/` contains a string literal with a controller-generated Pod
  name (`<name>-<template-hash>-<suffix>`, both drawn from Kubernetes' `SafeEncodeString` alphabet
  `bcdfghjklmnpqrstvwxz2456789`, hash 6-10, suffix 5). Hint: derive the Pod from the bound workload
  (owner relation) and use a typed exec (`EnsureExec`/`Command{Pod: ...}`), which needs no receipt.
- `playbook.duplicate-repair`: a playbook added in this diff (absent at base, not retired) has the same
  repair signature as another active playbook. Signature = ordered parameter Kinds plus the normalized
  AST of `Mitigate` and its package helpers (all but `Spec`/`Diagnose`): comments and package name
  dropped, local identifiers and role names canonicalized by first use / spec order, prose fields
  (`Reason`, `Rationale`, `Fact`, `Observed`, `Expected`, `Summary`, `Description`) erased, and the
  program ID and accepted-source object names replaced by placeholders. Exact match only, no
  thresholds. A copy of the whole program (Diagnose too) is rejected; repeating only the Mitigate is the
  non-blocking note `playbook.shared-repair` (see "Review changes"). Hint: refine the existing playbook
  instead of adding a per-symptom or per-service copy.
### (c) Skill guidance (same PRs as the behavior)

`sdo-playbook-authoring` and `sdo-detector-authoring`: a playbook is one repair method, named for the
repair (`redis-acl-restore`, `workload-restart`), not for a symptom or service; its roles bind the
component; a new cause for an existing repair widens that playbook's `Diagnose`; a new repair method is
a new playbook, not an extra action in a symptom playbook; never add a required role no detector binds
(derive Pods from the bound workload); gate diagnose on `Implicates`; when a recorded command names a
live Pod, encode a typed exec instead of `RunScript`. Rule references in `references/rules.md`.

## Review changes

An independent review found that the note blamed the wrong mechanism first and that two checks were unsafe:

- Over-binding comes from the SDK: `program.BindFinding` applies a finding's named `ParameterBindings` to every
  program with a role of that name, whatever the finding's `Playbooks` list says. The home-timeline detector's
  binding therefore reached the user-timeline program (C3: wrong Redis) and vice versa (B4). Fix the class in
  the SDK: named bindings apply only to programs the finding lists (or to all when it lists none). This fixes
  existing memory without relearning; unnamed binding from finding resources is unchanged.
- The validator itself steered C1 to a `redis-pod` role: the `reflection.program-required` hint told reflection
  to bind the receipt's Pod. The hint now names the Pod's recorded owning workload.
- A blocking "same Mitigate" rule would have rejected the first stall playbook in favour of
  `workload-stale-oom-restart` (identical Mitigate) and invited merging a vacuous Diagnose into a broad playbook.
  Now: an exact copy of a whole program (Diagnose and Mitigate) is rejected (`playbook.duplicate-repair`); a
  new playbook that repeats only the Mitigate is a non-blocking note (`playbook.shared-repair`).
- Folding (a new repair added to a symptom playbook) is not caught by any static rule; the skill text owns it,
  including "narrow a stale playbook's Diagnose off a cause it does not fix".
- Localization by subtracting healthy journeys is unsound when requests skip dependencies (cache hits, empty
  timelines); `localize` therefore ranks components (on every failing path; own reachability/link failures)
  instead of hard subtraction, consumes traffic and reachability explicitly, and treats a missing window as
  Unknown. It ships after the binding fix, which delivers most of the reuse-wave value.
- A second review (SWEEP1) found that keeping only the top score still subtracted: a culprit a healthy
  journey also lists (an ACL-revoked Redis that a healthy write lists, with TCP dials still succeeding)
  scored below its handler and was excluded. Suspects are now every component on every failing path plus
  every Service failing its own dials, scores only order them, and BindSuspects copies without a
  detector fingerprint get distinct ones. The cost is precision: a shared front end on every failing path
  is a suspect too, so a program must gate on its own evidence on the bound component, never on
  `Implicates` alone.
- `detector.literal-binding` is dropped: once named bindings are scoped to the detector's own playbooks, a
  literal binding no longer leaks into other programs.
- A second review (SWEEP1) found two gaps in that scoping. (1) `reflection.detector-required` still bound
  recorded findings as if they listed every playbook, so reflection could publish a reused playbook the
  firing finding never binds and pass the check. It now runs `program.BindFinding` with the program's own
  ID and the finding's playbook list (recorded, plus what its detector declares in the candidate tree, so
  editing the detector to list the playbook counts), and names the detector that would bind it if it
  listed the playbook. The rule stays a coverage note (never refused), as before; replay's
  `program.repeat-unreachable` note checks the edited detector code itself. (2) Scoping was opt-in: a finding
  that lists no playbook still offers its named bindings to every program. That broad offer stays for
  controller drift and source-inventory findings, which list none by design. A responder's incident
  detector that sets named bindings (`ParameterBindings`, `localize.BindSuspects`) must declare its
  playbooks: the blocking rule `detector.unscoped-binding` (changed incident detectors only) replaces the
  dropped literal-binding rule. A literal binding in a scoped detector reaches only that detector's own
  playbooks, so it is still allowed.
- `program.BindFinding` compares a finding's playbooks by logical ID only (the `Finding.Playbooks`
  contract the controller enforces); its own path normalizer, a second copy of `layout.PlaybookID`, is gone.

## Programs that cannot observe their own repair (added scope)

PX1 found `user-timeline-request-stall` reading `kubectl.kubernetes.io/restartedAt` from
`DiffAgainstSource(...).Field(...)`. Source comparisons strip that annotation, so the Diagnose matches after its
own restart, repeats it and loses attribution. The requested gate, "replay each playbook's plan on its corpus case
and require Diagnose to stop matching", would make replay verdicts a publication gate, which CLAUDE.md forbids
("replay outcomes are informational"); that is a decision for the user. What ships instead fixes the class
statically and deterministically: `program.normalized-read` rejects changed program code that looks up a stripped
bookkeeping annotation in a value derived from a source/baseline comparison. The validator's annotation list is
pinned to the controller's comparison policy by a runtime parity test. The live-object read
(`home-timeline-request-stall`) passes. The rule follows values by variable, not by name (a
shadowed or reused name is a different variable), ignores a comparison's error result and text built by
`fmt`/`errors`/`log`, and does not judge a variable that is also assigned anything else: it may miss a read,
but it does not reject a valid program for a name it shares.

## Testing (deterministic)

- `sdk/localize`: table tests over synthetic traffic windows and reachability windows (single journey,
  shared dependency, empty intersection, unqualified scenario, reachability narrowing, StatefulSet
  backing), plus `sdktest` snapshot use from a detector and a program.
- Validator: gate-table cases with hand-written synthetic memory trees (pass/fail per rule: new copy of
  an existing repair modulo names and prose; refinement of an existing duplicate passes; distinct field
  paths stay distinct; Pod-name literal in mitigate/script/detector; StatefulSet `-0` names and Services
  pass; literal vs relation-derived binding).
- Worked examples in `sdo/skills/sdo-playbook-authoring/assets/examples/` keep passing the memory check
  (`worked_examples_test.go`); skill lint and prompt-budget tests.

## Will not do

- No runtime/controller change (no automatic `localized` binding source, no candidate expansion, no
  recovery_wait change); PX1 owns those files. Possible follow-up once (a) shows value.
- No proto/contract change, no LLM or fuzzy similarity, no similarity thresholds.
- No automatic merge, retirement or migration of existing duplicate playbooks or detectors.
- No change to health detectors, the judge's traffic generators, or SREGym/benchmark code.
- No live runs (the orchestrator runs the reuse wave).
